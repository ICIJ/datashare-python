import asyncio
from collections.abc import AsyncGenerator, Generator, Iterable
from functools import partial

from aiostream.stream import chain
from datashare_python.objects import Document
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    to_raw_progress,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    COUNT,
    DOC_CONTENT,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    TERM,
    ESClient,
    has_id,
    must_not,
)
from temporalio.client import Client
from transformers import Pipeline, pipeline

from .objects_ import TranslationConfig
from .utils_ import async_batches, batches, before_and_after, once


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
    # @with_progress
    # @positional_args_only
    async def create_translation_batches(
        self, project: str, target_language: str, batch_size: int
    ) -> list[list[str]]:
        return await create_translation_batches(
            project=project,
            target_language=target_language,
            batch_size=batch_size,
            es_client=self._es_client,
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
    # @with_progress
    # @positional_args_only
    def translate_docs(
        self,
        docs: list[str],
        target_language: str,
        # *,
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
) -> None:
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
