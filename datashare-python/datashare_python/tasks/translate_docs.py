import logging
from functools import partial
from typing import AsyncGenerator, Generator, Iterable

import torch
from aiostream.stream import chain
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    COUNT,
    DOC_CONTENT,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    ESClient,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    TERM,
    has_id,
    must_not,
)
from icij_worker.typing_ import PercentProgress
from icij_worker.utils.progress import to_raw_progress
from transformers import Pipeline, pipeline

from datashare_python.task_client import DatashareTaskClient
from datashare_python.constants import PYTHON_TASK_GROUP
from datashare_python.objects import Document, TranslationConfig
from datashare_python.tasks.dependencies import lifespan_es_client, lifespan_task_client
from datashare_python.utils import async_batches, batches, before_and_after, once

logger = logging.getLogger(__name__)


async def create_translation_tasks(
    *,
    project: str,
    target_language: str,
    config: TranslationConfig | None = None,
    es_client: ESClient | None = None,
    task_client: DatashareTaskClient | None = None,
) -> list[str]:
    if es_client is None:
        es_client = lifespan_es_client()
    if task_client is None:
        task_client = lifespan_task_client()
    task_ids = []
    if config is None:
        config = TranslationConfig()
    # Retrieve unprocessed docs.
    docs_by_language = _untranslated_by_language(
        es_client, project, target_language=target_language
    )
    args = {
        "project": project,
        "config": config.dict(),
        "target_language": target_language,
    }
    # We could set this to a smarter value
    task_batch_size = config.batch_size * 4
    current_language = None
    async for language_docs in docs_by_language:
        async for batch in async_batches(language_docs, task_batch_size):
            language = batch[0][SOURCE][DOC_LANGUAGE]
            batch = [doc[ID_] for doc in batch]
            if language != current_language:
                logger.info("creating translation task for docs in %s", language)
            args["docs"] = batch
            task_id = await task_client.create_task(
                "translate_docs", args, group=PYTHON_TASK_GROUP.name
            )
            task_ids.append(task_id)
    logger.info("done creating %s translation tasks", len(task_ids))
    return task_ids


_TRANSLATION_DOC_SOURCES = [DOC_CONTENT, DOC_ROOT_ID, DOC_LANGUAGE]


async def translate_docs(
    docs: list[str],
    target_language: str,
    *,
    project: str,
    es_client: ESClient | None = None,
    progress: PercentProgress | None = None,
    config: TranslationConfig = TranslationConfig(),
) -> int:
    if es_client is None:
        es_client = lifespan_es_client()
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
            batch_docs.extend((Document.from_es(doc) for doc in page[HITS][HITS]))
        if pipe is None:
            source_language = batch_docs[0].language
            kwargs = config.to_pipeline_args(
                source_language, target_language=target_language
            )
            pipe = pipeline(device=device, **kwargs)
        # Load the classification pipeline
        contents = [d.content for d in batch_docs]
        translations = _translate(pipe, contents)
        await _add_translation(
            es_client,
            zip(batch_docs, translations),
            project,
            target_language=target_language,
        )
        seen += len(batch)
        if progress is not None:
            await progress(seen)
    # Return the number of classified documents
    return n_docs


def _translate(pipe: Pipeline, texts: list[str]) -> Generator[str, None, None]:
    for res in pipe(texts):
        yield res["translation_text"]


def _has_language(doc: dict, language: str) -> bool:
    return doc[SOURCE][DOC_LANGUAGE] == language


async def _untranslated_by_language(
    es_client: ESClient, project: str, target_language: str
) -> AsyncGenerator[AsyncGenerator[list[str], None], None]:
    docs = _get_untranslated(es_client, project, target_language=target_language)
    while True:
        try:
            next_doc = await anext(aiter(docs))
        except StopAsyncIteration:
            return
        current_language = next_doc[SOURCE][DOC_LANGUAGE]
        language_docs, docs = before_and_after(
            docs, partial(_has_language, language=current_language)
        )
        yield chain(once(next_doc), language_docs)


_SCRIPT_SOURCES = """
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
):
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            "_routing": doc.root_document,
            ID_: doc.id,
            "script": {
                "source": _SCRIPT_SOURCES,
                "lang": "painless",
                "params": {"language": target_language, "translation": translation},
            },
        }
        for doc, translation in translations
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


def _untranslated_query(target_language: str):
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
