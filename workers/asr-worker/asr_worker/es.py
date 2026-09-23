import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from pathlib import Path
from typing import Any

from aiofile import async_open
from datashare_python.objects import DocRoute, Document, WorkerPaths
from datashare_python.types_ import AsyncProgressRateHandler
from datashare_python.utils import (
    artifact_path,
    publish_and_consume,
    symlink_embedded_document_to_workdir,
    to_raw_async_progress,
    write_batches,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    DOC_CONTENT,
    DOC_CONTENT_TYPE,
    DOC_EXTRACTION_LEVEL,
    DOC_LANGUAGE,
    DOC_METADATA,
    DOC_PATH,
    DOC_ROOT_ID,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    ESClient,
    and_query,
    has_type,
)
from icij_common.iter_utils import async_batches

from asr_worker.objects import (
    ASRIndexingConfig,
    DocRoutes,
    Transcription,
    TranscriptionArtifact,
)

from .constants import SUPPORTED_CONTENT_TYPES

_EXCLUDED_FROM_BATCH_SERIALIZATION = {"type", "tags"}

logger = logging.getLogger(__name__)


async def search_audios_act(
    project: str,
    paths: WorkerPaths,
    es_client: ESClient,
    query: dict[str, Any],
    *,
    output_dir: Path,
    batch_size: int,
) -> AsyncIterable[Path]:
    # TODO: supported content types should be args
    docs = _search_audio_paths(
        es_client, project, query, supported_content_types=SUPPORTED_CONTENT_TYPES
    )
    docs = (symlink_embedded_document_to_workdir(a, paths) async for a in docs)
    docs = async_batches(docs, batch_size)
    async for p in write_batches(docs, output_dir, prefix="audio_"):
        yield p


# TODO: try to reduce the number of args here
async def index_transcriptions_act(  # noqa: PLR0917
    routes: DocRoutes,
    project: str,
    es_client: ESClient,
    artifact_root: Path,
    target_bulk_char_size: int = 100_000,
    es_concurrency: int = 5,
    indexing_config: ASRIndexingConfig | None = None,
    progress: AsyncProgressRateHandler | None = None,
) -> int:
    if indexing_config is None:
        indexing_config = ASRIndexingConfig()
    es_queue = asyncio.Queue(maxsize=es_concurrency)
    publisher = _read_transcriptions_and_queue(
        routes,
        es_queue,
        project,
        target_bulk_char_size,
        artifact_root=artifact_root,
        indexing_config=indexing_config,
        progress=progress,
    )
    publisher = asyncio.create_task(publisher)
    publisher_callback = lambda: es_queue.put_nowait(None)  # noqa: E731
    consumer = asyncio.create_task(
        _write_transcriptions_to_es(es_client, queue=es_queue, project=project)
    )
    n_indexed, _ = await publish_and_consume(
        publisher, publisher_callback, consumer=consumer
    )
    return n_indexed


_DOC_TYPE_QUERY = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
_DOC_CONTENT_SOURCES = [
    DOC_PATH,
    DOC_ROOT_ID,
    DOC_LANGUAGE,
    DOC_METADATA,
    DOC_ROOT_ID,
    DOC_EXTRACTION_LEVEL,
]


async def _search_audio_paths(
    es_client: ESClient,
    project: str,
    query: dict[str, Any],
    supported_content_types: set[str],
) -> AsyncGenerator[Document, None]:
    body = _with_audio_content(query, supported_content_types)
    async for page in es_client.poll_search_pages(
        index=project, body=body, sort="_doc:asc", _source_includes=_DOC_CONTENT_SOURCES
    ):
        for hit in page[HITS][HITS]:
            yield Document.from_es(hit)


def _content_type_query(supported_content_types: set[str]) -> dict[str, Any]:
    content_type_query = {"terms": {DOC_CONTENT_TYPE: sorted(supported_content_types)}}
    doc_type = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
    return and_query(content_type_query, doc_type)


def _with_audio_content(
    query: dict[str, Any], supported_content_types: set[str]
) -> dict[str, Any]:
    type_query = _content_type_query(supported_content_types)
    if not query:
        return type_query
    return and_query(query, type_query[QUERY])


async def _read_transcriptions_and_queue(
    routes: DocRoutes,
    queue: asyncio.Queue,
    project: str,
    target_bulk_char_size: int,
    indexing_config: ASRIndexingConfig,
    *,
    artifact_root: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> int:
    n_docs = len(routes)
    if not n_docs:
        return n_docs
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    bulk = []
    bulk_char_size = 0
    for doc_i, route in enumerate(routes.items()):
        doc_id, _ = route
        transcription_path = artifact_path(
            doc_id,
            TranscriptionArtifact,
            project=project,
            root=artifact_root,
        )
        async with async_open(transcription_path) as f:
            transcription = Transcription.model_validate_json(await f.read())
        indexed = transcription.as_text(
            indexing_config.transcript_sep, speaker_sep=indexing_config.speaker_sep
        )
        if bulk_char_size + len(indexed) >= target_bulk_char_size and bulk:
            await queue.put(bulk)
            bulk = []
            logger.debug("queued %s / %s transcription for indexation !", doc_i, n_docs)
        bulk.append((route, indexed))
        if progress is not None and doc_i % 10 == 0:
            await progress(doc_i)
    # Empty the buffer
    if bulk:
        await queue.put(bulk)
    if progress is not None:
        await progress(n_docs)
    queue.put_nowait(None)
    return n_docs


async def _write_transcriptions_to_es(
    es_client: ESClient, queue: asyncio.Queue, project: str
) -> None:
    while True:
        transcriptions = await queue.get()
        if transcriptions is None:
            logger.debug("popped poison pill from the queue, exiting !")
            queue.task_done()
            return
        logger.debug("writing translations to the index..")
        await _update_docs_content(es_client, transcriptions, project=project)
        logger.debug("translation written !")
        queue.task_done()


async def _update_docs_content(
    es_client: ESClient,
    transcribed_docs: Iterable[tuple[DocRoute, str]],
    project: str,
) -> None:
    actions = (
        {
            "_op_type": "update",
            "_index": project,
            "_routing": routing,
            ID_: doc_id,
            "doc": {DOC_CONTENT: transcription},
        }
        for (routing, doc_id), transcription in transcribed_docs
    )
    await async_bulk(es_client, actions, raise_on_error=True, refresh="wait_for")
