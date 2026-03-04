import asyncio
import logging
from collections.abc import AsyncGenerator, Generator, Iterable
from functools import partial

from aiostream.stream import chain
from datashare_python.objects import Document
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    to_raw_progress,
    to_scaled_progress,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    COUNT,
    DOC_CONTENT,
    DOC_CONTENT_TRANSLATED,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    HITS,
    ID_,
    MUST_NOT,
    QUERY,
    SHOULD,
    SOURCE,
    TERM,
    UPDATE,
    ESClient,
    and_query,
    bulk_action,
    has_id,
    must_not,
)
from temporalio import activity
from temporalio.client import Client
from transformers import Pipeline, pipeline

from .objects_ import ClassificationConfig, TranslationConfig
from .utils_ import async_batches, batches, before_and_after, once


class Pong(ActivityWithProgress):
    @activity_defn(name="pong")
    async def pong(self) -> str:
        return "pong"


class CreateTranslationBatches(ActivityWithProgress):
    def __init__(
        self,
        es_client: ESClient,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(temporal_client, event_loop)
        self._es_client = es_client

    @activity_defn(name="create-translation-batches")
    async def create_translation_batches(
        self, project: str, target_language: str, batch_size: int
    ) -> list[list[str]]:
        return await create_translation_batches(
            project=project,
            target_language=target_language,
            batch_size=batch_size,
            es_client=self._es_client,
        )


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


class TranslateDocs(ActivityWithProgress):
    def __init__(
        self,
        es_client: ESClient,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(temporal_client, event_loop)
        self._es_client = es_client

    @activity_defn(name="translate-docs")
    def translate_docs(
        self,
        docs: list[str],
        target_language: str,
        *,
        project: str,
        config: TranslationConfig,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        return self._event_loop.run_until_complete(
            translate_docs(
                docs,
                target_language=target_language,
                project=project,
                es_client=self._es_client,
                config=config,
                progress=progress,
            )
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


async def create_translation_batches(
    *,
    project: str,
    target_language: str,
    batch_size: int,
    es_client: ESClient | None = None,
) -> list[list[str]]:
    # Retrieve unprocessed docs.
    docs_by_language = _untranslated_by_language(
        es_client, project, target_language=target_language
    )
    # We could set this to a smarter value
    task_batch_size = batch_size * 4
    batches = []
    # Iterate on all documents by language
    async for language_docs in docs_by_language:
        # Create batches per language
        async for batch in async_batches(language_docs, task_batch_size):
            doc_ids = [doc[ID_] for doc in batch]
            batches.append(doc_ids)
    return batches


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


_TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]


async def translate_docs(
    docs: list[str],
    target_language: str,
    *,
    project: str,
    es_client: ESClient | None = None,
    progress: ProgressRateHandler | None = None,  # noqa: F821
    config: TranslationConfig | None = None,
) -> int:
    import torch  # noqa:PLC0415

    if config is None:
        config = TranslationConfig()

    # TODO: this should not happen
    if not isinstance(config, TranslationConfig):
        config = TranslationConfig.model_validate(config)

    n_docs = len(docs)
    if not n_docs:
        return 0
    # Torch/macOS silicon stuff
    device = None
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    seen = 0
    # Convert the progress to a "raw" progress to update the progress incrementally
    # rather than setting the progress rate
    if progress is not None:
        progress = to_raw_progress(progress, max_progress=n_docs)
    pipe = None
    # We batch the data ourselves, ideally, we should use an async version of:
    # https://huggingface.co/docs/datasets/v3.1.0/en/package_reference/main_classes#datasets.Dataset.from_generator
    for batch in batches(docs, batch_size=config.batch_size):
        batch_docs = []
        async for page in es_client.poll_search_pages(
            body={QUERY: has_id(batch)},
            _source_includes=_TRANSLATION_DOC_SOURCES,
        ):
            batch_docs.extend(Document.from_es(doc) for doc in page[HITS][HITS])
        if pipe is None:
            source_language = batch_docs[0].language
            kwargs = config.to_pipeline_args(
                source_language, target_language=target_language
            )
            pipe = pipeline(device=device, **kwargs)
        # Load the classification pipeline
        contents = [d.content for d in batch_docs]
        translations = await asyncio.to_thread(_translate_as_list, pipe, contents)
        await _add_translation(
            es_client,
            zip(batch_docs, translations, strict=False),
            project,
            target_language=target_language,
        )
        seen += len(batch)
        if progress is not None:
            await progress(seen)
    # Return the number of classified documents
    return n_docs


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


def _translate_as_list(pipe: Pipeline, texts: list[str]) -> list[str]:
    return list(_translate(pipe, texts))


def _translate(pipe: Pipeline, texts: list[str]) -> Generator[str, None, None]:
    for res in pipe(texts):
        yield res["translation_text"]


def _has_language(doc: dict, language: str) -> bool:
    return doc[SOURCE][DOC_LANGUAGE] == language


async def _untranslated_by_language(
    es_client: ESClient, project: str, target_language: str
) -> AsyncGenerator[AsyncGenerator[dict, None], None]:
    # Get all documents that are not in the target language sorted by language
    docs = _get_untranslated(es_client, project, target_language=target_language)
    while True:
        try:
            next_doc = await anext(aiter(docs))
        except StopAsyncIteration:
            return
        current_language = next_doc[SOURCE][DOC_LANGUAGE]
        # Consume the iterator until we find a doc with a different language
        language_docs, docs = before_and_after(
            docs, partial(_has_language, language=current_language)
        )
        # Yield all docs of the same language
        yield chain(once(next_doc), language_docs)


_TRANSLATION_SCRIPT_SOURCES = """
if( !ctx._source.containsKey("content_translated") ) {
    ctx._source.content_translated = new HashMap();
}
ctx._source.content_translated[params.language] = params.translation;
"""


async def _add_translation(
    es_client: ESClient,
    translations: Iterable[tuple[Document, str]],
    project: str,
    *,
    target_language: str,
) -> None:
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            "_routing": doc.root_document,
            ID_: doc.id,
            "script": {
                "source": _TRANSLATION_SCRIPT_SOURCES,
                "lang": "painless",
                "params": {"language": target_language, "translation": translation},
            },
        }
        for doc, translation in translations
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


def _untranslated_query(target_language: str) -> dict:
    query = {
        "query": {
            BOOL: must_not(
                {"exists": {"field": f"content_translated.{target_language}"}},
                {TERM: {DOC_LANGUAGE: target_language}},
            )
        }
    }
    return query


async def _get_untranslated(
    es_client: ESClient, project: str, *, target_language: str
) -> AsyncGenerator[dict, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=_untranslated_query(target_language),
        _source_includes=[DOC_LANGUAGE],
        sort=[f"{DOC_LANGUAGE}:asc", "_doc:asc"],
    ):
        for hit in res[HITS][HITS]:
            yield hit


async def _count_untranslated(
    es_client: ESClient, project: str, *, target_language: str
) -> int:
    res = await es_client.count(
        index=project, body=_untranslated_query(target_language)
    )
    return res[COUNT]


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


_CLASSIFICATION_SCRIPT_SOURCES = """
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
                "source": _CLASSIFICATION_SCRIPT_SOURCES,
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


ACTIVITIES = [
    CreateTranslationBatches.create_translation_batches,
    CreateClassificationBatches.create_classification_batches,
    TranslateDocs.translate_docs,
    ClassifyDocs.classify_docs,
    Pong.pong,
]
