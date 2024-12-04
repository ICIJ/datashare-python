from functools import partial
from typing import AsyncGenerator, Generator, Iterable

import pycountry
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
    TERM,
)
from icij_worker.typing_ import PercentProgress
from icij_worker.utils.progress import to_raw_progress
from transformers import Pipeline, pipeline

from ml_worker.objects import Document, TranslationConfig
from ml_worker.utils import async_batches, before_and_after, once


async def translate_docs(
    target_language: str,
    es_client: ESClient,
    project: str,
    progress: PercentProgress | None = None,
    config: TranslationConfig = TranslationConfig(),
) -> int:
    # Retrieve docs which haven't been classified yet
    n_docs = await _count_untranslated(
        es_client, project, target_language=target_language
    )
    if not n_docs:
        if progress is not None:
            await progress(1.0)
        return n_docs
    # Convert the progress to a "raw" progress to update the progress incrementally
    # rather than setting the progress rate
    progress = to_raw_progress(progress, max_progress=n_docs)
    # Torch/macOS silicon stuff
    device = None
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    seen = 0
    docs_by_language = _untranslated_by_language(
        es_client, project, target_language=target_language
    )
    target_lang_alpha_2 = pycountry.languages.get(name=target_language).alpha_2
    async for language_docs in docs_by_language:
        # We batch the data ourselves, ideally, we should use an async version of:
        # https://huggingface.co/docs/datasets/v3.1.0/en/package_reference/main_classes#datasets.Dataset.from_generator
        async for batch in async_batches(language_docs, batch_size=config.batch_size):
            source_lang_alpha_2 = pycountry.languages.get(
                name=batch[0].language
            ).alpha_2
            # Load the classification pipeline
            pipe = pipeline(
                device=device,
                **config.to_pipeline_args(
                    source_lang_alpha_2, target_language=target_lang_alpha_2
                ),
            )
            contents = [d.content for d in batch]
            translations = _translate(pipe, contents)
            await _add_translation(
                es_client,
                zip(batch, translations),
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


def _has_language(doc: Document, language: str) -> bool:
    return doc.language == language


async def _untranslated_by_language(
    es_client: ESClient, project: str, target_language: str
) -> AsyncGenerator[AsyncGenerator[tuple[Document], None], None]:
    docs = _get_untranslated(es_client, project, target_language=target_language)
    while True:
        try:
            next_doc = await anext(docs)
        except StopAsyncIteration:
            return
        current_language = next_doc.language
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
            BOOL: {
                "must_not": [
                    {"exists": {"field": f"content_translated.{target_language}"}},
                    {TERM: {DOC_LANGUAGE: target_language}},
                ]
            }
        }
    }
    return query


_DOC_SOURCES = [DOC_LANGUAGE, DOC_CONTENT, DOC_ROOT_ID, "project"]


async def _get_untranslated(
    es_client: ESClient, project: str, *, target_language: str
) -> AsyncGenerator[Document, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=_untranslated_query(target_language),
        _source_includes=_DOC_SOURCES,
        sort=[f"{DOC_LANGUAGE}:asc", "_doc:asc"],
    ):
        for res in res[HITS][HITS]:
            yield Document.from_es(res)


async def _count_untranslated(
    es_client: ESClient, project: str, *, target_language: str
) -> int:
    res = await es_client.count(
        index=project, body=_untranslated_query(target_language)
    )
    return res[COUNT]
