import asyncio
from collections import defaultdict
from collections.abc import AsyncGenerator, Iterable
from functools import partial
from typing import Any

from aiostream.stream import chain
from datashare_python.objects import Document
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    async_batches,
    batches,
    before_and_after,
    once,
    to_raw_progress,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    COUNT,
    DOC_LANGUAGE,
    HITS,
    ID_,
    SOURCE,
    TERM,
    ESClient,
    must_not,
)
from temporalio.client import Client

from .constants import (
    LANGUAGE_ALPHA_CODE_ACTIVITY_NAME,
    TRANSLATION_BATCHING_ACTIVITY_NAME,
    TRANSLATION_DOC_SOURCES,
    TRANSLATION_TRANSLATE_ACTIVITY_NAME,
)
from .objects import (
    AsyncEsDocumentIteratorByLanguage,
    BatchSentence,
    SentencesBatch,
    TranslationConfig,
    TranslationEnsemble,
)
from .utils import (
    _get_translation_package,
    _has_language,
    _language_alpha_codes,
    _translate_as_list,
)


class CreateTranslationBatches(ActivityWithProgress):
    def __init__(
        self,
        es_client: ESClient,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(temporal_client, event_loop)
        self._es_client = es_client

    @activity_defn(name=LANGUAGE_ALPHA_CODE_ACTIVITY_NAME)
    async def language_alpha_codes(self, *languages: str) -> list[str]:
        return _language_alpha_codes(*languages)

    @activity_defn(name=TRANSLATION_BATCHING_ACTIVITY_NAME)
    async def create_translation_batches(
        self, project: str, target_language_alpha_code: str, config: TranslationConfig
    ) -> list[SentencesBatch]:
        return await create_translation_batches(
            project=project,
            target_language_alpha_code=target_language_alpha_code,
            config=config,
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

    @activity_defn(name=TRANSLATION_TRANSLATE_ACTIVITY_NAME)
    def translate_sentences(
        self,
        sentence_batch: list[dict[str, Any]],
        target_language_alpha_code: str,
        *,
        project: str,
        config: TranslationConfig,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        return self._event_loop.run_until_complete(
            translate_sentences(
                sentence_batch,
                target_language_alpha_code=target_language_alpha_code,
                project=project,
                es_client=self._es_client,
                config=config,
                progress=progress,
            )
        )


async def _process_language_group(
    language_docs_iter: AsyncEsDocumentIteratorByLanguage,
    target_language_alpha_code: str,
    config: TranslationConfig,
) -> AsyncGenerator[SentencesBatch, None]:
    source_language_alpha_code = language_docs_iter.language
    docs_iter = language_docs_iter.document_iter
    translation_package = _get_translation_package(
        source_language_alpha_code=source_language_alpha_code,
        target_language_alpha_code=target_language_alpha_code,
        config=config,
    )

    async for tupled_batch in async_batches(
        _iter_sentences(docs_iter, translation_package, source_language_alpha_code),
        config.batch_size,
    ):
        # async_batches returns tuples
        for batch in tupled_batch:
            yield batch


async def _iter_sentences(
    docs_iter: AsyncGenerator[dict, None],
    translation_package: TranslationEnsemble,
    source_language: str,
) -> AsyncGenerator[SentencesBatch, None]:
    async for doc in docs_iter:
        es_doc = Document.from_es(doc)
        sentences = translation_package.sentencizer.split_sentences(es_doc.content)
        sentences = [
            BatchSentence(
                doc_id=es_doc.id,
                root_document=es_doc.root_document,
                sentence_index=i,
                sentence=sentence,
            )
            for i, sentence in enumerate(sentences)
        ]

        yield SentencesBatch(
            language=source_language,
            sentences=sentences,
        )


async def _collect_language_batches(
    language_docs: AsyncEsDocumentIteratorByLanguage,
    target_language_alpha_code: str,
    config: TranslationConfig,
) -> list[SentencesBatch]:
    result = []
    async for batch in _process_language_group(
        language_docs, target_language_alpha_code, config
    ):
        result.append(batch)

    return result


async def create_translation_batches(
    *,
    project: str,
    target_language_alpha_code: str,
    config: TranslationConfig,
    es_client: ESClient | None = None,
) -> list[SentencesBatch]:
    """Split texts by sentence and batch them

    :param project: Project name
    :param target_language_alpha_code: Target language
    :param config: TranslationConfig
    :param es_client: ES client
    :return: list of SentencesBatches
    """
    # Retrieve unprocessed docs.
    docs_by_language = _untranslated_by_language(
        es_client, project, target_language_alpha_code=target_language_alpha_code
    )
    # We could set this to a smarter value
    tasks = []

    async for language_docs in docs_by_language:
        tasks.append(
            asyncio.create_task(
                _collect_language_batches(
                    language_docs,
                    target_language_alpha_code=target_language_alpha_code,
                    config=config,
                )
            )
        )

    results = await asyncio.gather(*tasks)
    return [batch for group in results for batch in group]


async def translate_sentences(
    sentences_batch: list[dict[str, Any]],
    target_language_alpha_code: str,
    *,
    project: str,
    es_client: ESClient | None = None,
    progress: ProgressRateHandler | None = None,  # noqa: F821
    config: TranslationConfig | None = None,
) -> int:
    """Translate sentence batches and reconstruct translations from original
    sentence ordering, inserting them into ES

    :param sentences_batch: Sentences batch
    :param target_language_alpha_code: Target language alpha2 code
    :param project: Project name
    :param es_client: ES client
    :param progress: ProgressRateHandler
    :param config: TranslationConfig | None
    :return: number of documents translated
    """
    if config is None:
        config = TranslationConfig()

    # TODO: this should not happen
    if not isinstance(config, TranslationConfig):
        config = TranslationConfig.model_validate(config)

    # Rehydrate for convenience
    sentences_batch = SentencesBatch.model_validate(sentences_batch)
    sentences = sentences_batch.sentences
    n_sentences = len(sentences)
    if not n_sentences:
        return 0

    # Convert the progress to a "raw" progress to update the progress incrementally
    # rather than setting the progress rate
    if progress is not None:
        progress = to_raw_progress(progress, max_progress=n_sentences)
    # We batch the data ourselves, ideally, we should use an async version of:
    # https://huggingface.co/docs/datasets/v3.1.0/en/package_reference/main_classes#datasets.Dataset.from_generator
    source_language_alpha_code = sentences_batch.language
    # unit here is a sentence
    seen = 0
    total = n_sentences * 2

    # Translate
    all_translations = []

    translation_tasks = [
        asyncio.create_task(
            _translate_batch(
                sentence_batch,
                source_language_alpha_code,
                target_language_alpha_code,
                config,
            )
        )
        for sentence_batch in batches(sentences, batch_size=config.batch_size)
    ]

    for task in asyncio.as_completed(translation_tasks):
        translation_batch = await task
        all_translations.extend(translation_batch)

        seen += len(translation_batch)

        if progress is not None:
            await progress(int(seen / total))

    all_translations = await asyncio.gather(*translation_tasks)
    all_translations = [
        translation for batch in all_translations for translation in batch
    ]

    # Reconstruct documents from sentences
    reconstructed_docs = defaultdict(dict)

    for batch_sentence, translation in zip(sentences, all_translations, strict=False):
        key = batch_sentence.doc_id, batch_sentence.root_document
        reconstructed_docs[key][batch_sentence.sentence_index] = translation

    # Combine sentences into translations and key with doc_id and root_document
    # for insertion
    translations_with_doc_ids_and_root_doc = []

    for (doc_id, root_document), sentence_idx_mapping in reconstructed_docs.items():
        ordered_translation = " ".join(
            [translation for (_, translation) in sorted(sentence_idx_mapping.items())]
        )
        seen += len(ordered_translation)

        translations_with_doc_ids_and_root_doc.append(
            (doc_id, root_document, ordered_translation)
        )

        if progress is not None:
            await progress(int(seen / total))

    await _add_translation(
        es_client,
        translations_with_doc_ids_and_root_doc,
        project,
        target_language_alpha_code=target_language_alpha_code,
    )
    # Return the number of classified documents
    return len(reconstructed_docs)


# async
async def _translate_batch(
    sentence_batch: tuple[BatchSentence, ...],
    source_language_alpha_code: str,
    target_language_alpha_code: str,
    config: TranslationConfig,
) -> list[str]:
    async with asyncio.Semaphore(config.max_parallel_batches):
        return await asyncio.to_thread(
            _translate_as_list,
            sentence_batch,
            source_language_alpha_code,
            target_language_alpha_code,
            config,
        )


async def _untranslated_by_language(
    es_client: ESClient, project: str, target_language_alpha_code: str
) -> AsyncGenerator[AsyncEsDocumentIteratorByLanguage, None]:
    # Get all documents that are not in the target language sorted by language
    docs = _get_untranslated(
        es_client, project, target_language_alpha_code=target_language_alpha_code
    )
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
        grouped_docs = chain(once(next_doc), language_docs)

        source_language_alpha_code = _language_alpha_codes(current_language)[0]

        yield AsyncEsDocumentIteratorByLanguage(
            language=source_language_alpha_code,
            document_iter=grouped_docs,
        )


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
    target_language_alpha_code: str,
) -> None:
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            "_routing": root_document,
            ID_: doc_id,
            "script": {
                "source": _SCRIPT_SOURCES,
                "lang": "painless",
                "params": {
                    "language": target_language_alpha_code,
                    "translation": translation,
                },
            },
        }
        for doc_id, root_document, translation in translations
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


def _untranslated_query(target_language_alpha_code: str) -> dict:
    query = {
        "query": {
            BOOL: must_not(
                {
                    "exists": {
                        "field": f"content_translated.{target_language_alpha_code}"
                    }
                },
                {TERM: {DOC_LANGUAGE: target_language_alpha_code}},
            )
        }
    }
    return query


async def _get_untranslated(
    es_client: ESClient, project: str, *, target_language_alpha_code: str
) -> AsyncGenerator[dict, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=_untranslated_query(target_language_alpha_code),
        _source_includes=TRANSLATION_DOC_SOURCES,
        sort=[f"{DOC_LANGUAGE}:asc", "_doc:asc"],
    ):
        for hit in res[HITS][HITS]:
            yield hit


async def _count_untranslated(
    es_client: ESClient, project: str, *, target_language_alpha_code: str
) -> int:
    res = await es_client.count(
        index=project, body=_untranslated_query(target_language_alpha_code)
    )
    return res[COUNT]


ACTIVITIES = [CreateTranslationBatches, TranslateDocs]
