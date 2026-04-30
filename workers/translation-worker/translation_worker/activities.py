import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from copy import deepcopy
from functools import partial
from typing import Any, cast

from aiostream.stream import chain
from datashare_python.dependencies import lifespan_es_client, lifespan_worker_config
from datashare_python.objects import Document
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import ActivityWithProgress, activity_defn, to_raw_progress
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    BOOL,
    DOC_LANGUAGE,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    TERM,
    ESClient,
    has_id,
    must_not,
)
from icij_common.iter_utils import before_and_after, once
from pydantic_extra_types.language_code import LanguageAlpha2, LanguageName

from .constants import BATCHING_DOC_SOURCES, CONTENT_LENGTH, TRANSLATION_DOC_SOURCES
from .core import get_translation_ensemble, has_language, translate_as_list
from .objects import BatchSentence, TranslationEnsemble, TranslationWorkerConfig

logger = logging.getLogger(__name__)


class TranslationActivities(ActivityWithProgress):
    @activity_defn(name="translation.worker_config")
    async def translation_worker_config(self) -> TranslationWorkerConfig:
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        return worker_config

    @activity_defn(name="translation.create_translation_batches")
    async def create_translation_batches(
        self, project: str, target_language: LanguageAlpha2
    ) -> list[tuple[str, list[list[str]]]]:
        es_client = lifespan_es_client()
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        max_batch_byte_len = worker_config.max_batch_byte_len
        batches = await create_translation_batches(
            project=project,
            target_language=target_language,
            max_batch_byte_len=max_batch_byte_len,
            es_client=es_client,
        )
        return batches

    @activity_defn(name="translation.translate_docs")
    async def translate_docs(
        self,
        doc_id_batch_with_lang: tuple[str, list[list[str]]],
        target_language: str,
        *,
        project: str,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        es_client = lifespan_es_client()
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        n_translated = await translate_docs(
            doc_id_batch_with_lang,
            target_language=target_language,
            project=project,
            es_client=es_client,
            progress=progress,
            worker_config=worker_config,
        )
        return n_translated


async def create_translation_batches(
    *,
    project: str,
    target_language: LanguageAlpha2,
    max_batch_byte_len: int = 1000000,
    es_client: ESClient | None = None,
) -> list[tuple[str, list[list[str]]]]:
    """Batch doc ids by language and/or total batch byte length

    :param project: Project name
    :param target_language: Target language
    :param max_batch_byte_len: Maximum batch byte length
    :param es_client: ES client
    :return: list of batches keyed by language
    """
    # Retrieve unprocessed docs.
    es_docs = _get_es_docs(
        es_client,
        project,
        target_language_alpha_code=target_language,
        source_includes=BATCHING_DOC_SOURCES,
    )
    all_results = {}
    current_batch = []
    current_batch_byte_len = 0

    async for es_doc_id_batch in es_docs:
        first_doc = await anext(es_doc_id_batch, None)
        if first_doc is None:
            continue

        source_alpha_2 = LanguageName(first_doc[SOURCE][DOC_LANGUAGE].title()).alpha2
        all_results[source_alpha_2] = []
        current_batch.append(first_doc[ID_])

        async for item in es_doc_id_batch:
            doc_id = item[ID_]
            doc_byte_len = item[SOURCE][CONTENT_LENGTH]

            if 0 < max_batch_byte_len < current_batch_byte_len + doc_byte_len:
                all_results[source_alpha_2].append(deepcopy(current_batch))
                current_batch = []
                current_batch_byte_len = 0

            current_batch.append(doc_id)
            current_batch_byte_len += doc_byte_len

        if len(current_batch) > 0:
            all_results[source_alpha_2].append(deepcopy(current_batch))
            current_batch = []
            current_batch_byte_len = 0

    return list(all_results.items())


async def translate_docs(
    doc_id_batch_with_lang: tuple[str, list[list[str]]],
    target_language: LanguageAlpha2,
    *,
    project: str,
    es_client: ESClient | None = None,
    worker_config: TranslationWorkerConfig | None = None,
    progress: ProgressRateHandler | None = None,  # noqa: F821
) -> int:
    """Translate sentence batches and reconstruct translations from original
    sentence ordering, inserting them into ES

    :param doc_id_batch_with_lang: doc_ids keyed by document language alpha code
    :param target_language: Target language alpha2 code
    :param project: Project name
    :param es_client: ES client
    :param progress: ProgressRateHandler
    :param worker_config: worker config
    :return: number of documents translated
    """
    if worker_config is None:
        worker_config = TranslationWorkerConfig()

    # TODO: this should not happen
    if not isinstance(worker_config, TranslationWorkerConfig):
        worker_config = TranslationWorkerConfig.model_validate(worker_config)

    source_language_alpha_code, doc_id_batches = doc_id_batch_with_lang

    # Get documents
    translation_ensemble = get_translation_ensemble(
        source_language_alpha_code=source_language_alpha_code,
        target_language_alpha_code=target_language,
        device=worker_config.device,
        inter_threads=worker_config.inter_threads,
        intra_threads=worker_config.intra_threads,
        compute_type=worker_config.compute_type,
    )

    all_sentences = []
    all_translations = []
    translation_tasks = []

    # unit here is a sentence
    seen = 0
    total = 0

    for doc_id_batch in doc_id_batches:
        sentences_batches = _get_doc_contents_and_split_on_sentences(
            es_client,
            project,
            doc_id_batch,
            translation_ensemble,
            worker_config.batch_size,
        )

        # Create translation tasks
        async for sentences_batch in sentences_batches:
            n_sentences = len(sentences_batch)
            if not n_sentences:
                continue

            # Convert the progress to a "raw" progress to update the progress
            # incrementally rather than setting the progress rate
            if progress is not None:
                progress = to_raw_progress(progress, max_progress=n_sentences)
            total += n_sentences

            # Translate
            translation_tasks.append(
                asyncio.create_task(
                    _translate_batch(
                        sentences_batch, translation_ensemble, worker_config.beam_size
                    )
                )
            )

            all_sentences += sentences_batch

    # Run translation tasks
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
    # TODO: separate into a function for testing
    reconstructed_docs = defaultdict(dict)

    for batch_sentence, translation in zip(
        all_sentences, all_translations, strict=False
    ):
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
        target_language_alpha_code=target_language,
    )
    # Return the number of translated documents
    return len(reconstructed_docs)


# async
async def _get_doc_contents_and_split_on_sentences(
    es_client: ESClient,
    project: str,
    doc_ids: list[str],
    translation_ensemble: TranslationEnsemble,
    sentence_batch_size: int = 16,
) -> AsyncGenerator[list[BatchSentence] | None, Any]:
    if len(doc_ids) == 0:
        return

    batch_gen = _async_query_es(
        es_client,
        project,
        body={QUERY: has_id(doc_ids)},
        source_includes=TRANSLATION_DOC_SOURCES,
    )

    async for batch in _iter_sentences(
        batch_gen, translation_ensemble, sentence_batch_size
    ):
        yield batch


async def _iter_sentences(
    doc_iter: AsyncGenerator[dict, None],
    translation_ensemble: TranslationEnsemble,
    sentence_batch_size: int = 16,
) -> AsyncGenerator[list[BatchSentence], None]:
    sentence_batch = []

    async for doc in doc_iter:
        es_doc = Document.from_es(doc)
        sentences = await asyncio.to_thread(
            translation_ensemble.sentencizer.split_sentences, es_doc.content
        )
        for idx, sentence in enumerate(sentences):
            sentence_batch.append(
                BatchSentence(
                    doc_id=es_doc.id,
                    root_document=es_doc.root_document,
                    sentence_index=idx,
                    sentence=sentence,
                )
            )

            if len(sentence_batch) >= sentence_batch_size:
                yield sentence_batch
                sentence_batch = []

    if len(sentence_batch) > 0:
        yield sentence_batch


async def _translate_batch(
    sentence_batch: list[BatchSentence],
    translation_ensemble: TranslationEnsemble,
    max_parallel_batches: int = 8,
    beam_size: int = 4,
) -> list[str]:
    async with asyncio.Semaphore(max_parallel_batches):
        return await asyncio.to_thread(
            translate_as_list, sentence_batch, translation_ensemble, beam_size
        )


async def _get_es_docs(
    es_client: ESClient,
    project: str,
    target_language_alpha_code: str,
    source_includes: list[str],
) -> AsyncGenerator[AsyncIterator[dict], None]:
    # Get all documents that are not in the target language sorted by language
    docs = _async_query_es(
        es_client,
        project,
        body=_untranslated_query(target_language_alpha_code),
        source_includes=source_includes,
        sort=[f"{DOC_LANGUAGE}:asc", "_doc:asc"],
    )
    while True:
        try:
            next_doc = await anext(aiter(docs))
        except StopAsyncIteration:
            return
        current_language = next_doc[SOURCE][DOC_LANGUAGE]

        # Consume the iterator until we find a doc with a different language
        language_docs, docs = before_and_after(
            docs, predicate=partial(has_language, language=current_language)
        )
        # Group all docs of same language
        grouped_docs = chain(once(next_doc), language_docs)

        yield aiter(grouped_docs)


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


async def _async_query_es(
    es_client: ESClient,
    project: str,
    *,
    body: dict,
    source_includes: list[str] = None,
    sort: list[str] = None,
) -> AsyncGenerator[dict, None]:
    async for res in es_client.poll_search_pages(
        index=project,
        body=body,
        _source_includes=source_includes,
        sort=sort,
    ):
        for hit in res[HITS][HITS]:
            yield hit


ACTIVITIES = [
    TranslationActivities.create_translation_batches,
    TranslationActivities.translate_docs,
]
