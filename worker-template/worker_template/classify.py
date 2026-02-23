import asyncio
import logging
from collections.abc import AsyncGenerator, Generator, Iterable

from datashare_python.objects import Document
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    to_raw_progress,
    to_scaled_progress,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    DOC_CONTENT,
    DOC_CONTENT_TRANSLATED,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    HITS,
    ID_,
    MUST_NOT,
    QUERY,
    SHOULD,
    TERM,
    UPDATE,
    ESClient,
    and_query,
    bulk_action,
    has_id,
)
from objects_ import ClassificationConfig
from temporalio import activity
from temporalio.client import Client
from transformers import Pipeline, pipeline
from types_ import ProgressRateHandler
from utils_ import batches


class CreateClassificationBatches(ActivityWithProgress):
    def __init__(
        self,
        es_client: ESClient,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(temporal_client, event_loop)
        self._es_client = es_client

    @activity_defn(name="create-classification-batches")
    async def create_classification_batches(
        self, project: str, target_language: str, config: ClassificationConfig
    ) -> list[list[str]]:
        return await create_classification_batches(
            project=project,
            language=target_language,
            config=config,
            es_client=self._es_client,
            logger=activity.logger,
        )


class ClassifyDocs(ActivityWithProgress):
    def __init__(
        self,
        es_client: ESClient,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(temporal_client, event_loop)
        self._es_client = es_client

    @activity_defn(name="classify-docs")
    async def classify_docs(
        self,
        docs: list[str],
        classified_language: str,
        # *,
        project: str,
        config: ClassificationConfig,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        return await classify_docs(
            docs,
            classified_language=classified_language,
            project=project,
            es_client=self._es_client,
            config=config,
            progress=progress,
        )


async def create_classification_batches(
    *,
    project: str,
    language: str,
    config: ClassificationConfig,
    es_client: ESClient,
    progress: ProgressRateHandler | None = None,
    logger: logging.Logger,
) -> list[list[str]]:
    if not isinstance(config, ClassificationConfig):
        config = ClassificationConfig.model_validate(config)
    # Retrieve unprocessed docs.
    model = config.model
    unclassified = _get_unclassified(
        es_client, project=project, language=language, model=model
    )
    unclassified = [d[ID_] async for d in unclassified]
    n_docs = len(unclassified)
    if not n_docs:
        logger.info("found not unclassified documents !")
        return []
    logger.info("found %s unclassified documents !", n_docs)
    fetch_unclassified_progress = 0.5
    if progress is not None:
        await progress(fetch_unclassified_progress)
    effective_batch_size = config.batch_size * config.batches_per_task
    if progress is not None:
        n_tasks = n_docs // config.batch_size
        # We scale the progress to post incremental progress updates from 0 to n_tasks
        progress = to_scaled_progress(progress, start=fetch_unclassified_progress)
        progress = to_raw_progress(progress, max_progress=n_tasks)
    logger.info("creating %s classification tasks...", effective_batch_size)
    # We create classification tasks which will be picked up by the workers
    clf_batches = []
    for batch in batches(unclassified, effective_batch_size):
        clf_batches.append(batch)
        if progress is not None:
            await progress(len(clf_batches))
    logger.info("created all classification tasks !")
    return clf_batches


_CLASSIF_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_CONTENT_TRANSLATED, DOC_LANGUAGE]


async def classify_docs(
    docs: list[str],
    *,
    classified_language: str,
    project: str,
    config: ClassificationConfig | None = None,
    progress: ProgressRateHandler | None = None,
    es_client: ESClient,
) -> int:
    import torch  # noqa: PLC0415

    if config is None:
        config = ClassificationConfig()
    # TODO: fix this, we should have a ClassificationConfig hered
    if not isinstance(config, ClassificationConfig):
        config = ClassificationConfig.model_validate(config)

    n_docs = len(docs)
    model = config.model
    # Torch/macOS silicon stuff
    device = None
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    # Load the classification pipeline
    pipe = pipeline(config.task, model=model, device=device)
    model = pipe.model.name_or_path
    # Convert the progress to a "raw" progress to update the progress incrementally
    # from 0 to n_docs (rather than 0.0 to 1.0)
    if progress is not None:
        progress = to_raw_progress(progress, max_progress=n_docs)
    seen = 0
    # We batch the data ourselves, ideally, we should use an async version of:
    # https://huggingface.co/docs/datasets/v3.1.0/en/package_reference/main_classes#datasets.Dataset.from_generator
    for batch in batches(docs, batch_size=config.batch_size):
        batch_length = len(batch)
        batch_docs = []
        async for page in es_client.poll_search_pages(
            body={QUERY: has_id(batch)},
            _source_includes=_CLASSIF_DOC_SOURCES,
        ):
            batch_docs.extend([Document.from_es(doc) for doc in page[HITS][HITS]])
        contents = (_get_language_content(d, classified_language) for d in batch_docs)
        batch_docs, contents = zip(
            *(
                (d, c)
                for d, c in zip(batch_docs, contents, strict=False)
                if c is not None
            ),
            strict=False,
        )
        batch_docs = tuple(batch_docs)
        # Offload CPU bound computation to a thread to avoid blocking the IO loop,
        # classification will happen in numpy/python (outside of Python's GIL reach)
        labels = await asyncio.to_thread(_classify_as_list, pipe, list(contents))
        # We add the classification results by updating the documents with new tags,
        # this could also be done using: https://github.com/ICIJ/datashare-tarentula
        await _add_classification_tags(
            es_client, zip(batch_docs, labels, strict=False), project, model=model
        )
        seen += batch_length
        if progress is not None:
            await progress(seen)
    # Return the number of classified documents
    return n_docs


def _classify(pipe: Pipeline, texts: list[str]) -> Generator[str, None, None]:
    # In practice, we should chunk the text
    for res in pipe(texts, padding=True, truncation=True):
        yield res["label"]


def _classify_as_list(pipe: Pipeline, texts: list[str]) -> list[str]:
    return list(_classify(pipe, texts))


def _get_language_content(doc: Document, language: str) -> str | None:
    if doc.language == language:
        return doc.content
    return doc.content_translated.get(language)


_SCRIPT_SOURCES = """
if( !ctx._source.containsKey("tags") ) {
    ctx._source.tags = [];
}
if( !ctx._source.tags.contains(params.tag) ) {
    ctx._source.tags.add(params.tag);
}
"""


async def _add_classification_tags(
    es_client: ESClient,
    tags: Iterable[tuple[Document, str]],
    project: str,
    *,
    model: str,
) -> None:
    actions = (
        bulk_action(
            op_type=UPDATE,
            index=project,
            id_=doc.id,
            routing=doc.root_document,
            script={
                "source": _SCRIPT_SOURCES,
                "lang": "painless",
                "params": {"tag": f"classified:{model}:{label}"},
            },
        )
        for doc, label in tags
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


def _unclassified_query(model: str, language: str) -> dict:
    queries = (
        # Get documents which aren't tagged yet
        {BOOL: {MUST_NOT: {"prefix": {"tags": {"value": f"classified:{model}:"}}}}},
        # And which are either in the model language or are translated in the model
        # language
        {
            BOOL: {
                SHOULD: [
                    {"exists": {"field": f"{DOC_CONTENT_TRANSLATED}.{language}"}},
                    {TERM: {DOC_LANGUAGE: language}},
                ]
            }
        },
    )
    query = and_query(*queries)
    return query


async def _get_unclassified(
    es_client: ESClient, project: str, *, language: str, model: str, **kwargs
) -> AsyncGenerator[dict, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=_unclassified_query(model, language=language),
        sort="_doc:asc",
        _source=False,
        **kwargs,
    ):
        for hit in res[HITS][HITS]:
            yield hit
