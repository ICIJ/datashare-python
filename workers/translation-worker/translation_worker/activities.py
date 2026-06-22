import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterable
from functools import partial
from typing import Any, cast

from aiostream.stream import chain
from datashare_python.dependencies import lifespan_es_client, lifespan_worker_config
from datashare_python.objects import DatashareLanguage, Document, Translation
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import ActivityWithProgress, activity_defn, to_raw_progress
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    DOC_CONTENT_TRANSLATED,
    DOC_LANGUAGE,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    ESClient,
    and_query,
    has_id,
    has_type,
)
from icij_common.iter_utils import async_batches, before_and_after, once
from pydantic import TypeAdapter

from translation_worker.constants import DOC_CONTENT_TEXT_LENGTH

from .config import TranslationWorkerConfig
from .constants import BATCHING_DOC_SOURCES, TRANSLATION_DOC_SOURCES
from .objects import Language, TranslationConfig
from .processors import SentenceSplitter, Translator

logger = logging.getLogger(__name__)

DocId = str
Batch = list[DocId]

_LANGUAGE_TYPE_ADAPTER = TypeAdapter(Language)


class TranslationActivities(ActivityWithProgress):
    @activity_defn(name="translation.worker-config")
    async def translation_worker_config(self) -> TranslationWorkerConfig:
        logger.info("loading worker configuration...")
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        return worker_config

    @activity_defn(name="translation.create-translation-batches")
    async def create_translation_batches(
        self, project: str, query: dict[str, Any]
    ) -> list[tuple[Language, list[Batch]]]:
        es_client = lifespan_es_client()
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        batch_text_length = worker_config.batch_text_length
        logger.info("creating translation batches...")
        batches = [
            b
            async for b in create_translation_batches_act(
                project,
                query,
                batch_text_length=batch_text_length,
                es_client=es_client,
            )
        ]
        logger.info("translation batches created !")
        return batches

    @activity_defn(name="translation.translate-docs")
    async def translate_docs(
        self,
        batches: list[Batch],
        *,
        source: DatashareLanguage,
        target: Language,
        config: TranslationConfig,
        project: str,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        es_client = lifespan_es_client()
        worker_config = cast(TranslationWorkerConfig, lifespan_worker_config())
        # TODO: make a generic fix using interceptors,
        #  see https://github.com/temporalio/sdk-python/issues/360
        source = _LANGUAGE_TYPE_ADAPTER.validate_python(source)
        target = _LANGUAGE_TYPE_ADAPTER.validate_python(target)
        if isinstance(config, dict):
            config = TranslationConfig.model_validate(config)
        # TODO: perform some caching here to avoid reloading
        translator = config.to_translator()
        sentence_splitter = config.to_sentence_splitter()
        # Load the translator first to install the SBD, then load the splitter
        logger.debug("loading %s -> %s translator...", source, target)
        with translator.load(source, target=target, worker_config=worker_config):
            logger.debug("loading %s sentence splitter...", source)
            with sentence_splitter.load(source):
                logger.info("translating %s batches...", len(batches))
                n_translated = await translate_docs_act(
                    batches,
                    project=project,
                    es_client=es_client,
                    progress=progress,
                    worker_config=worker_config,
                    translator=translator,
                    sentence_splitter=sentence_splitter,
                )
                logger.info("done translating !")
        return n_translated


async def create_translation_batches_act(
    project: str,
    query: dict[str, Any],
    batch_text_length: int = 1000000,
    es_client: ESClient | None = None,
) -> AsyncGenerator[tuple[DatashareLanguage, list[Batch]], None]:
    # Retrieve unprocessed docs.
    query = _with_doc_type(query)
    es_docs = _get_es_docs_by_language(
        es_client, project, query, source_includes=BATCHING_DOC_SOURCES
    )
    async for language_docs in es_docs:
        language_batches: list[Batch] = []
        current_batch = []
        current_length = 0
        current_language = None

        async for doc in language_docs:
            doc_id: str = doc[ID_]
            doc_length = doc[SOURCE][DOC_CONTENT_TEXT_LENGTH]
            if current_language is None:
                current_language = DatashareLanguage(doc[SOURCE][DOC_LANGUAGE])
                logger.debug("creating batches for %s docs...", current_language)

            next_length = current_length + doc_length
            if next_length > batch_text_length:
                language_batches.append(list(current_batch))
                current_batch = []

            current_batch.append(doc_id)
            current_length = next_length

        if current_batch:
            language_batches.append(list(current_batch))
        if current_language is None:
            continue
        yield current_language, language_batches


async def translate_docs_act(
    batches: list[Batch],
    *,
    project: str,
    translator: Translator,
    sentence_splitter: SentenceSplitter,
    worker_config: TranslationWorkerConfig,
    es_client: ESClient,
    progress: ProgressRateHandler | None = None,  # noqa: F821
) -> int:
    # TODO: this should not happen
    es_queue = asyncio.Queue()
    publisher = _translate_and_queue(
        batches,
        es_queue,
        project,
        translator,
        sentence_splitter,
        worker_config,
        es_client,
        progress,
    )
    publisher = asyncio.create_task(publisher)
    publisher_callback = lambda: es_queue.put_nowait(None)  # noqa: E731
    consumer = asyncio.create_task(
        _write_translations_to_es(es_client, queue=es_queue, project=project)
    )
    n_docs, _ = await _publish_and_consume(
        publisher, publisher_callback, consumer=consumer
    )
    return n_docs


async def _translate_and_queue(
    batches: list[Batch],
    queue: asyncio.Queue,
    project: str,
    translator: Translator,
    sentence_splitter: SentenceSplitter,
    worker_config: TranslationWorkerConfig,
    es_client: ESClient,
    progress: ProgressRateHandler | None = None,  # noqa: F821
) -> int:
    n_docs = sum(len(b) for b in batches)
    if not n_docs:
        return n_docs
    if progress is not None:
        progress = to_raw_progress(progress, max_progress=n_docs)
    source = translator.source
    target = translator.target
    model = translator.registered_name
    translation_factory = partial(
        Translation, source_language=source, target_language=target, translator=model
    )
    seen = 0
    buffer = []
    current_doc = None
    current_doc_translation = []
    n_batches = len(batches)
    for batch_i, doc_ids in enumerate(batches):
        logger.debug("translating batch %s / %s", batch_i, n_batches)
        docs = _poll_from_es(
            es_client,
            project,
            body={QUERY: has_id(doc_ids)},
            source_includes=TRANSLATION_DOC_SOURCES,
        )
        doc_sents = _split_sentences(docs, sentence_splitter)
        # TODO: ideally we should aim at having almost constant size batches,
        #  by using some sort of binarization / binning. That will also improve add
        #  context to translate short sentences. A split strategy
        #  like adding min and max batch_item length should help
        doc_sent_batches = async_batches(doc_sents, batch_size=worker_config.batch_size)
        async for batch in doc_sent_batches:
            batch_docs, sents = zip(*batch, strict=False)
            # Run translation 1 batch at the time, parallelization is controlled
            # via the batch_size
            translated_sents = await asyncio.to_thread(translator.translate, sents)
            for doc, translated_sent in zip(batch_docs, translated_sents, strict=True):
                if current_doc is not None and doc.id != current_doc.id:
                    translation = translation_factory(content=current_doc_translation)
                    buffer.append((current_doc, translation))
                    if len(buffer) >= worker_config.es_buffer_size:
                        queue.put_nowait(buffer)
                        buffer = []
                    seen += 1
                    if progress is not None:
                        await progress(seen)
                    current_doc_translation = []
                current_doc = doc
                current_doc_translation.append(translated_sent)
        logger.debug("batch %s / %s translated !", batch_i, n_batches)
    # Empty the buffer
    if current_doc_translation:
        translation = translation_factory(content=current_doc_translation)
        buffer.append((current_doc, translation))
        queue.put_nowait(buffer)
    return n_docs


async def _write_translations_to_es(
    es_client: ESClient, queue: asyncio.Queue, project: str
) -> None:
    while True:
        translated_docs = await queue.get()
        if translated_docs is None:
            logger.debug("popped poison pill from the queue, exiting !")
            queue.task_done()
            return
        logger.debug("writing translations to the index..")
        await _update_docs_translation(es_client, translated_docs, project=project)
        logger.debug("translation written !")
        queue.task_done()


async def _split_sentences(
    doc_iter: AsyncGenerator[dict, None], sentence_splitter: SentenceSplitter
) -> AsyncGenerator[tuple[Document, str], None]:
    async for doc in doc_iter:
        es_doc = Document.from_es(doc)
        sentences = await asyncio.to_thread(
            sentence_splitter.split_sentences, es_doc.content
        )
        for sentence in sentences:
            yield es_doc, sentence


async def _get_es_docs_by_language(
    es_client: ESClient,
    project: str,
    query: dict[str, Any],
    source_includes: list[str],
) -> AsyncGenerator[AsyncIterator[dict], None]:
    docs = _poll_from_es(
        es_client,
        project,
        body=query,
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
            docs, predicate=partial(_has_language, language=current_language)
        )
        # Group all docs of same language
        grouped_docs = chain(once(next_doc), language_docs)
        yield aiter(grouped_docs)


_SCRIPT_SOURCES = f"""
if (ctx._source.{DOC_CONTENT_TRANSLATED} == null) {{
    ctx._source.{DOC_CONTENT_TRANSLATED} = [params.translation];
}} else {{
    def existing = ctx._source.{DOC_CONTENT_TRANSLATED};
    for (int i = 0; i < existing.size(); i++) {{
        if (existing[i].source_language == params.translation.source_language
            && existing[i].target_language == params.translation.target_language) {{
            if (existing[i].content == params.translation.content) {{
                ctx.op = 'none';  // skip write if identical
                return;
            }}
            existing[i] = params.translation;
            return;
        }}
    }}
    existing.add(params.translation);
}}
"""


async def _update_docs_translation(
    es_client: ESClient,
    translated_docs: Iterable[tuple[Document, Translation]],
    project: str,
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
                "params": {"translation": translation.model_dump(by_alias=True)},
            },
        }
        for doc, translation in translated_docs
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")


async def _poll_from_es(
    es_client: ESClient,
    project: str,
    *,
    body: dict,
    source_includes: list[str] = None,
    sort: list[str] = None,
) -> AsyncGenerator[dict, None]:
    async for res in es_client.poll_search_pages(
        index=project, body=body, _source_includes=source_includes, sort=sort
    ):
        for hit in res[HITS][HITS]:
            yield hit


def _has_language(doc: dict, language: str) -> bool:
    return doc[SOURCE][DOC_LANGUAGE] == language


def _with_doc_type(query: dict[str, Any]) -> dict[str, Any]:
    return and_query(query, has_type(type_field="type", type_value=ES_DOCUMENT_TYPE))


async def _publish_and_consume(
    publisher: asyncio.Task,
    publisher_completion_callback: Callable[[], None],
    *,
    consumer: asyncio.Task,
) -> tuple[Any, Any]:
    # Publish and consume concurrently
    logger.debug("starting publish and subscribe")
    done, pending = await asyncio.wait(
        [publisher, consumer], return_when=asyncio.FIRST_COMPLETED
    )
    for d in done:
        # Stop everything case of exception
        exc = d.exception()
        if exc:
            for p in pending:
                p.cancel()
            raise exc
    # Wait for publish to be done and push the poison pill to stop consuming
    p_res = await publisher
    publisher_completion_callback()
    logger.debug("done publishing, waiting for consumer to complete...")
    # Wait for consumption to be done
    c_res = await consumer
    logger.debug("done consuming !")
    return p_res, c_res


ACTIVITIES = [
    TranslationActivities.translation_worker_config,
    TranslationActivities.create_translation_batches,
    TranslationActivities.translate_docs,
]
