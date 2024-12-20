import logging
from typing import AsyncGenerator, Generator, Iterable, Optional

import torch
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    DOC_CONTENT,
    DOC_CONTENT_TRANSLATED,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    ESClient,
    HITS,
    ID_,
    MUST_NOT,
    QUERY,
    SHOULD,
    TERM,
    UPDATE,
    and_query,
    bulk_action,
    has_id,
)
from icij_worker.ds_task_client import DatashareTaskClient
from icij_worker.typing_ import PercentProgress
from icij_worker.utils.progress import to_raw_progress, to_scaled_progress
from transformers import Pipeline, pipeline

from datashare_python.constants import PYTHON_TASK_GROUP
from datashare_python.objects import ClassificationConfig, Document
from datashare_python.tasks.dependencies import lifespan_es_client, lifespan_task_client
from datashare_python.utils import batches

logger = logging.getLogger(__name__)


async def create_classification_tasks(
    *,
    project: str,
    language: str,
    n_workers: int,
    config: ClassificationConfig | None,
    es_client: ESClient | None = None,
    task_client: DatashareTaskClient | None = None,
    progress: PercentProgress | None = None,
) -> list[str]:
    if n_workers < 1:
        raise ValueError("n_workers must be at least 1")
    if es_client is None:
        es_client = lifespan_es_client()
    if task_client is None:
        task_client = lifespan_task_client()
    task_ids = []
    if config is None:
        config = ClassificationConfig()
    # Retrieve unprocessed docs.
    model = config.model
    unclassified = _get_unclassified(
        es_client, project=project, language=language, model=model
    )
    unclassified = [d[ID_] async for d in unclassified]
    n_docs = len(unclassified)
    if not n_docs:
        logger.info("found not unclassified documents !")
        return task_ids
    logger.info("found %s unclassified documents !", n_docs)
    fetch_unclassified_progress = 0.5
    if progress is not None:
        await progress(fetch_unclassified_progress)
    # Roughly split the load between workers:
    # - they should approximately receive the same amount of work
    # - they should receive tasks which are long enough to avoid model loading overhead
    # - task should be short enough to avoid starting all over again from scratch in
    # case of failure
    n_tasks = max(n_docs // n_workers, n_docs // (n_workers * 5), 1)
    task_batch_size = n_docs // n_tasks
    if progress is not None:
        # We scale the progress to post incremental progress updates from 0 to n_tasks
        progress = to_scaled_progress(progress, start=fetch_unclassified_progress)
        progress = to_raw_progress(progress, max_progress=n_tasks)
    logger.info("creating %s classification tasks...", n_tasks)
    # We create classification tasks which will be picked up by the workers
    args = {"project": project, "config": config.dict(), "language": language}
    for batch in batches(unclassified, task_batch_size):
        args["docs"] = batch
        task_id = await task_client.create_task(
            "classify_docs", args, group=PYTHON_TASK_GROUP.name
        )
        task_ids.append(task_id)
        if progress is not None:
            await progress(len(task_ids))
    logger.info("created all classification tasks !")
    return task_ids


_CLASSIF_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_CONTENT_TRANSLATED, DOC_LANGUAGE]


async def classify_docs(
    docs: list[str],
    *,
    language: str,
    project: str,
    config: ClassificationConfig = ClassificationConfig(),
    progress: PercentProgress | None = None,
    es_client: ESClient | None = None,
) -> int:
    if es_client is None:
        es_client = lifespan_es_client()
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
        contents = (_get_language_content(d, language) for d in batch_docs)
        batch_docs, contents = zip(
            *((d, c) for d, c in zip(batch_docs, contents) if c is not None)
        )
        batch_docs = tuple(batch_docs)
        labels = _classify(pipe, list(contents))
        # We add the classification results by updating the documents with new tags,
        # this could also be done using: https://github.com/ICIJ/datashare-tarentula
        await _add_classification_tags(
            es_client, zip(batch_docs, labels), project, model=model
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


def _get_language_content(doc: Document, language: str) -> Optional[str]:
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
):
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


def _unclassified_query(model: str, language: str):
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
