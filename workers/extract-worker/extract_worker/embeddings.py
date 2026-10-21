import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterable, Iterator
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol

from datashare_python.constants import MANIFEST_JSON
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    ByteRangesPagination,
    Document,
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
)
from datashare_python.types_ import AsyncProgressRateHandler
from datashare_python.utils import (
    artifact_path,
    artifacts_dir,
    async_read_jsonl_as,
    to_raw_async_progress,
)
from elasticsearch._async.helpers import async_bulk
from icij_common.es import (
    ES_DOCUMENT_TYPE,
    ID_,
    QUERY,
    ESClient,
    and_query,
    has_id,
    has_type,
)
from temporalio import activity

from .config import EmbeddingWorkerConfig
from .objects import (
    DocId,
    DocumentSearchQuery,
    EmbeddedChunk,
    EmbeddingError,
    EmbeddingResponse,
    StructureArtifact,
    StructureManifestEntry,
)

logger = logging.getLogger(__name__)

_DOC_ID = "docId"
_EMBEDDING = "embedding"


class MissingEmbeddingsIndex(ValueError):
    def __init__(self, index: str) -> None:
        super().__init__(f"embeddings index {index} doesn't exist, create it first")
        self.index = index


class EmbeddingSizeMismatch(ValueError):
    def __init__(self, index: str, expected: int, actual: int | None) -> None:
        if actual is None:
            msg = f"embeddings index {index} has no {_EMBEDDING} vector field"
        else:
            msg = (
                f"embeddings index {index} expects vectors of size {actual},"
                f" requested size is {expected}"
            )
        super().__init__(msg)
        self.index = index
        self.expected = expected
        self.actual = actual


class Embedder(Protocol):
    model: str

    def embed(self, texts: list[str], dim: int) -> list[list[float]]: ...


class HFEmbedder:
    """Generate embeddings using a HuggingFace model exposing a jina-embeddings-like
    `encode` method (task + prompt name)
    """

    def __init__(
        self,
        config: EmbeddingWorkerConfig,
        model: str,
        model_revision: str | None = None,
    ) -> None:
        import torch  # noqa: PLC0415
        from transformers import AutoModel  # noqa: PLC0415

        self.model = model
        self._task = config.task
        self._batch_size = config.inference_batch_size
        device = config.device.value
        dtype = torch.bfloat16 if device == "cuda" else torch.float32
        self._model = AutoModel.from_pretrained(
            model,
            revision=model_revision,
            trust_remote_code=True,
            dtype=dtype,
        ).to(device)

    def embed(self, texts: list[str], dim: int) -> list[list[float]]:
        embeddings = []
        for start in range(0, len(texts), self._batch_size):
            batch = texts[start : start + self._batch_size]
            batch_embeddings = self._model.encode(
                texts=batch,
                task=self._task,
                prompt_name="document",
                truncate_dim=dim,
            )
            embeddings.extend(batch_embeddings.float().cpu().tolist())
        return embeddings


def make_chunker(
    config: EmbeddingWorkerConfig, model: str, model_revision: str | None = None
) -> "HybridChunker":  # noqa: F821
    from docling_core.transforms.chunker.hybrid_chunker import (  # noqa: PLC0415
        HybridChunker,
    )
    from docling_core.transforms.chunker.tokenizer.huggingface import (  # noqa: PLC0415
        HuggingFaceTokenizer,
    )

    # use the embedding model tokenizer so that chunks fit the model context
    tokenizer = HuggingFaceTokenizer.from_pretrained(
        model, max_tokens=config.max_tokens, revision=model_revision
    )
    return HybridChunker(tokenizer=tokenizer)


def make_md_converter() -> "DocumentConverter":  # noqa: F821
    from docling.datamodel.base_models import InputFormat  # noqa: PLC0415
    from docling.document_converter import DocumentConverter  # noqa: PLC0415

    return DocumentConverter(allowed_formats=[InputFormat.MD])


async def embed_documents_act(
    batch: Path,
    *,
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
    embedder: Embedder,
    dim: int,
    artifacts_root: Path,
    output_path: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> EmbeddingResponse:
    docs = [d async for d in async_read_jsonl_as(batch, PROCESSED_FILE_TA)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    n_pages, n_successes, n_successes_pages, n_chunks = 0, 0, 0, 0
    errors = []
    with output_path.open("w") as f:
        for doc_i, doc in enumerate(docs):
            # to avoid heartbeat timeout
            with contextlib.suppress(RuntimeError):
                activity.heartbeat()
            n_pages += doc.n_pages
            try:
                # run chunking and inference in a separate thread to avoid
                # blocking the event loop
                chunks = await asyncio.to_thread(
                    _embed_doc,
                    doc,
                    chunker=chunker,
                    md_converter=md_converter,
                    embedder=embedder,
                    dim=dim,
                    artifacts_root=artifacts_root,
                )
            except Exception as e:  # noqa: BLE001
                logger.exception("failed to embed doc %s", doc.doc_id)
                errors.append(EmbeddingError.from_exception(doc, e))
            else:
                for chunk in chunks:
                    f.write(f"{chunk.model_dump_json()}\n")
                n_chunks += len(chunks)
                n_successes += 1
                n_successes_pages += doc.n_pages
            if progress is not None:
                await progress(doc_i + 1)
    processed = ProcessingReportWithPages(n_docs=n_docs, n_pages=n_pages)
    successes = ProcessingReportWithPages(n_docs=n_successes, n_pages=n_successes_pages)
    errors = ErrorReportWithPages.from_errors(*errors)
    return EmbeddingResponse(
        processed=processed, successes=successes, errors=errors, n_chunks=n_chunks
    )


async def index_embeddings_act(
    chunks_path: Path,
    *,
    es_client: ESClient,
    index: str,
    dim: int,
    bulk_size: int,
) -> int:
    await ensure_embeddings_index(es_client, index, dim=dim)
    chunks = [c async for c in async_read_jsonl_as(chunks_path, EmbeddedChunk)]
    if not chunks:
        return 0
    # chunking might have changed, we don't want stale chunks to remain,
    # remove previous chunks
    doc_ids = sorted({c.doc_id for c in chunks})
    delete_query = {QUERY: {"terms": {_DOC_ID: doc_ids}}}
    await es_client.delete_by_query(
        index=index, body=delete_query, conflicts="proceed", refresh=True
    )
    n_indexed = 0
    for start in range(0, len(chunks), bulk_size):
        bulk = chunks[start : start + bulk_size]
        actions = (_index_action(c, index) for c in bulk)
        n_success, _ = await async_bulk(
            es_client, actions, raise_on_error=True, refresh="wait_for"
        )
        n_indexed += n_success
    logger.debug("indexed %s chunks for %s docs in %s", n_indexed, len(doc_ids), index)
    return n_indexed


def embeddings_index_body(dim: int) -> dict[str, Any]:
    keyword = {"type": "keyword"}
    properties = {
        _DOC_ID: keyword,
        "rootDocument": keyword,
        "project": keyword,
        "chunkIndex": {"type": "integer"},
        "page": {"type": "integer"},
        "text": {"type": "text"},
        "headings": {"type": "text"},
        "model": keyword,
        _EMBEDDING: {"type": "dense_vector", "dims": dim},
    }
    return {"mappings": {"dynamic": "strict", "properties": properties}}


async def ensure_embeddings_index(es_client: ESClient, index: str, *, dim: int) -> None:
    """Check that the embeddings index exists and stores vectors of the right size.

    :param es_client: ES client
    :param index: name of the embeddings index
    :param dim: expected embedding vector size
    :raises MissingEmbeddingsIndex: when the index doesn't exist
    :raises EmbeddingSizeMismatch: when the index vector size doesn't match dim
    """
    if not await es_client.indices.exists(index=index):
        raise MissingEmbeddingsIndex(index)
    mappings = await es_client.indices.get_mapping(index=index)
    # index can be an alias, in which case the key is the concrete index name
    (mapping,) = mappings.values()
    properties = mapping["mappings"].get("properties", dict())
    actual = properties.get(_EMBEDDING, dict()).get("dims")
    if actual != dim:
        raise EmbeddingSizeMismatch(index, expected=dim, actual=actual)


def _index_action(chunk: EmbeddedChunk, index: str) -> dict[str, Any]:
    return {
        "_op_type": "index",
        "_index": index,
        "_routing": chunk.root_document,
        ID_: chunk.es_id,
        "_source": chunk.model_dump(by_alias=True),
    }


def _embed_doc(
    doc: ProcessedFile,
    *,
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
    embedder: Embedder,
    dim: int,
    artifacts_root: Path,
) -> list[EmbeddedChunk]:
    page_chunks = [
        (page, chunker.contextualize(chunk), chunk.meta.headings)
        for page, md in read_structure_pages(doc, artifacts_root)
        for chunk in _chunk_markdown(md, chunker, md_converter)
    ]
    if not page_chunks:
        return []
    embeddings = embedder.embed([text for _, text, _ in page_chunks], dim)
    return [
        EmbeddedChunk(
            doc_id=doc.doc_id,
            root_document=doc.root_document,
            project=doc.project,
            chunk_index=chunk_i,
            page=page,
            text=text,
            headings=headings,
            model=embedder.model,
            embedding=embedding,
        )
        for chunk_i, ((page, text, headings), embedding) in enumerate(
            zip(page_chunks, embeddings, strict=True)
        )
    ]


def _chunk_markdown(
    md: bytes,
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
) -> Iterator["BaseChunk"]:  # noqa: F821
    from docling.datamodel.base_models import DocumentStream  # noqa: PLC0415

    if not md.strip():
        return
    stream = DocumentStream(name="page.md", stream=BytesIO(md))
    dl_doc = md_converter.convert(stream).document
    yield from chunker.chunk(dl_doc)


def read_structure_pages(
    doc: ProcessedFile, artifacts_root: Path
) -> Iterator[tuple[int, bytes]]:
    """Read the markdown structure artifact of a document page by page.

    :param doc: document to read the structure of
    :param artifacts_root: root of the artifacts directory
    :return: (page index, page markdown bytes) pairs
    :raises FileNotFoundError: when the document has no markdown structure artifact
    """
    structure_dir = artifact_path(
        doc.doc_id, StructureArtifact, project=doc.project, root=artifacts_root
    )
    md_path = _structure_md_path(structure_dir)
    if md_path is None:
        raise FileNotFoundError(f"no markdown structure found for {doc.doc_id}")
    content = md_path.read_bytes()
    byte_ranges = _structure_byte_ranges(doc, artifacts_root)
    if not byte_ranges:
        yield 0, content
        return
    for page_i, (start, end) in enumerate(byte_ranges):
        yield page_i, content[start:end]


def _structure_md_path(structure_dir: Path) -> Path | None:
    if not structure_dir.is_dir():
        return None
    return next(structure_dir.glob("*.md"), None)


def _structure_byte_ranges(
    doc: ProcessedFile, artifacts_root: Path
) -> list[tuple[int, int]] | None:
    manifest_path = (
        artifacts_root / artifacts_dir(doc.doc_id, project=doc.project) / MANIFEST_JSON
    )
    if not manifest_path.exists():
        return None
    manifest = json.loads(manifest_path.read_text())
    entry = manifest.get(StructureArtifact.type)
    if entry is None:
        return None
    entry = StructureManifestEntry.model_validate(entry)
    pagination = entry.pages.pagination
    if not isinstance(pagination, ByteRangesPagination):
        return None
    return pagination.byte_ranges


def build_embedding_doc_query(
    docs: list[DocId] | DocumentSearchQuery | None,
) -> dict[str, Any]:
    doc_type_query = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
    match docs:
        case dict():
            if not docs:
                return {QUERY: doc_type_query}
            return and_query(doc_type_query, docs)
        case None:
            return {QUERY: doc_type_query}
        case list():
            return and_query(doc_type_query, has_id(docs))
        case _:
            raise ValueError(f"unsupported format {docs.__class__.__name__}")


async def with_structure_artifact(
    docs: AsyncIterable[Document], artifacts_root: Path
) -> AsyncIterable[Document]:
    async for d in docs:
        structure_dir = artifact_path(
            d.id, StructureArtifact, project=d.project, root=artifacts_root
        )
        if _structure_md_path(structure_dir) is None:
            logger.debug("skipping %s (no markdown structure)", d.id)
            continue
        yield d
