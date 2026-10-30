from pathlib import Path

import asyncio
import json
import logging
import re
from abc import ABC, abstractmethod
from aiofile import async_open
from collections.abc import AsyncIterable, Iterable, Iterator
from datashare_python.constants import MANIFEST_JSON
from datashare_python.objects import (
    ByteRangesPagination,
    DatashareFile,
    Document,
    ErrorReportWithPages,
    PROCESSED_FILE_TA,
    ProcessedFile,
    ProcessingReportWithPages,
)
from datashare_python.types_ import AsyncProgressRateHandler
from datashare_python.utils import (
    artifact_path,
    artifacts_dir,
    async_read_jsonl_as,
    reports_errors,
    to_raw_async_progress,
)
from elasticsearch._async.helpers import async_bulk
from functools import cache
from icij_common.es import (
    DOC_CONTENT,
    ESClient,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    and_query,
    has_id,
    has_type,
)
from io import BytesIO
from typing import Any

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
_CHUNK_INDEX = "chunkIndex"
_DELETE_GROUP_SIZE = 256
_PARAGRAPH_SEP = r"\n\s*\n"

# TODO: this is way too wide, this must be restricted
_EMBEDDING_RECOVERABLES = (Exception,)


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


class Embedder(ABC):
    def __init__(self, config: EmbeddingWorkerConfig) -> None:
        self._config = config

    @property
    def config(self) -> EmbeddingWorkerConfig:
        return self._config

    @property
    @abstractmethod
    def model(self) -> str: ...

    @abstractmethod
    async def embed(self, texts: list[str], dim: int) -> list[list[float]]: ...


class HFEmbedder(Embedder):
    """Generate embeddings using a HuggingFace model exposing a jina-embeddings-like
    `encode` method (task + prompt name)
    """

    def __init__(
        self,
        config: EmbeddingWorkerConfig,
        *,
        model: str,
        model_revision: str | None = None,
    ) -> None:
        super().__init__(config)
        import torch  # noqa: PLC0415
        from transformers import AutoModel  # noqa: PLC0415

        self.__model = model
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

    @property
    def model(self) -> str:
        return self.__model

    async def embed(self, texts: list[str], dim: int) -> list[list[float]]:
        embeddings = []
        for start in range(0, len(texts), self._batch_size):
            batch = texts[start : start + self._batch_size]
            batch_embeddings = await asyncio.to_thread(
                self._model.encode,
                texts=batch,
                task=self._task,
                prompt_name="document",
                truncate_dim=dim,
            )
            embeddings.extend(batch_embeddings.float().cpu().tolist())
        return embeddings


def make_chunker(
    config: EmbeddingWorkerConfig, *, model: str, model_revision: str | None = None
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
) -> tuple[EmbeddingResponse, dict[DocId, int]]:
    docs = [d async for d in async_read_jsonl_as(batch, PROCESSED_FILE_TA)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    n_pages, n_successes, n_successes_pages, n_chunks = 0, 0, 0, 0
    errors = []
    n_chunks_by_doc = dict()
    doc_chunks = _embed_doc_chunks(
        chunker, md_converter, embedder, dim, artifacts_root=artifacts_root
    )
    doc_i = 0
    async with async_open(output_path, "w") as f:
        for res in doc_chunks:
            if isinstance(res, EmbeddingError):
                doc = res.source.doc_id
                errors.append(res)
            else:
                doc, chunks = doc
                for chunk in chunks:
                    await f.write(f"{chunk.model_dump_json()}\n")
                n_chunks += len(chunks)
                n_chunks_by_doc[doc.doc_id] = len(chunks)
                n_successes += 1
                n_successes_pages += doc.n_pages
            doc_i += 1
            if progress is not None:
                await progress(doc_i)
    processed = ProcessingReportWithPages(n_docs=n_docs, n_pages=n_pages)
    successes = ProcessingReportWithPages(n_docs=n_successes, n_pages=n_successes_pages)
    errors = ErrorReportWithPages.from_errors(*errors)
    response = EmbeddingResponse(
        processed=processed, successes=successes, errors=errors, n_chunks=n_chunks
    )
    return response, n_chunks_by_doc


@reports_errors(_EMBEDDING_RECOVERABLES, EmbeddingError)
async def _embed_doc_chunks(
    docs: Iterable[ProcessedFile],
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
    embedder: Embedder,
    dim: int,
    *,
    artifacts_root: Path,
) -> AsyncIterable[tuple[ProcessedFile, list[EmbeddedChunk]]]:
    for doc in docs:
        chunks = await _embed_doc(
            doc,
            chunker=chunker,
            md_converter=md_converter,
            embedder=embedder,
            dim=dim,
            artifacts_root=artifacts_root,
        )
        yield doc, chunks


async def index_embeddings_act(
    chunks_path: Path,
    n_chunks_by_doc: dict[DocId, int],
    *,
    es_client: ESClient,
    index: str,
    dim: int,
    bulk_size: int,
) -> int:
    await ensure_embeddings_index(es_client, index, dim=dim)
    actions = (
        _index_action(c, index)
        async for c in async_read_jsonl_as(chunks_path, EmbeddedChunk)
    )
    n_indexed, _ = await async_bulk(
        es_client,
        actions,
        chunk_size=bulk_size,
        stats_only=True,
        raise_on_error=True,
        refresh="wait_for",
    )
    await _delete_stale_chunks(es_client, index, n_chunks_by_doc)
    logger.debug(
        "indexed %s chunks for %s docs in %s", n_indexed, len(n_chunks_by_doc), index
    )
    return n_indexed


async def _delete_stale_chunks(
    es_client: ESClient, index: str, n_chunks_by_doc: dict[DocId, int]
) -> None:
    docs = sorted(n_chunks_by_doc.items())
    for start in range(0, len(docs), _DELETE_GROUP_SIZE):
        group = docs[start : start + _DELETE_GROUP_SIZE]
        stale = [
            {
                "bool": {
                    "filter": [
                        {"term": {_DOC_ID: doc_id}},
                        {"range": {_CHUNK_INDEX: {"gte": n_chunks}}},
                    ]
                }
            }
            for doc_id, n_chunks in group
        ]
        query = {"bool": {"should": stale, "minimum_should_match": 1}}
        await es_client.delete_by_query(
            index=index, body={QUERY: query}, conflicts="proceed", refresh=True
        )


def embeddings_index_body(dim: int) -> dict[str, Any]:
    keyword = {"type": "keyword"}
    properties = {
        _DOC_ID: keyword,
        "rootDocument": keyword,
        "project": keyword,
        _CHUNK_INDEX: {"type": "integer"},
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


async def _embed_doc(
    doc: ProcessedFile,
    *,
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
    embedder: Embedder,
    dim: int,
    artifacts_root: Path,
) -> list[EmbeddedChunk]:
    chunks = list(page_chunks(doc, chunker, md_converter, artifacts_root))
    if not chunks:
        return []
    # embed chunks with their headings for context, but only store the chunk text
    texts = [chunker.contextualize(chunk) for _, chunk in chunks]
    embeddings = await embedder.embed(texts, dim)
    return [
        EmbeddedChunk(
            doc_id=doc.doc_id,
            root_document=doc.root_document,
            project=doc.project,
            chunk_index=chunk_i,
            page=page,
            text=chunk.text,
            headings=chunk.meta.headings,
            model=embedder.model,
            embedding=embedding,
        )
        for chunk_i, ((page, chunk), embedding) in enumerate(
            zip(chunks, embeddings, strict=True)
        )
    ]


def page_chunks(
    doc: ProcessedFile,
    chunker: "BaseChunker",  # noqa: F821
    md_converter: "DocumentConverter",  # noqa: F821
    artifacts_root: Path,
) -> Iterator[tuple[int, "BaseChunk"]]:  # noqa: F821
    content = _fallback_content(doc)
    if content is not None:
        for chunk in chunk_text(content, chunker):
            yield 0, chunk
        return
    for page, md in read_structure_pages(doc, artifacts_root):
        for chunk in _chunk_markdown(md, chunker, md_converter):
            yield page, chunk


def _fallback_content(doc: ProcessedFile) -> str | None:
    if isinstance(doc, DatashareFile):
        return doc.parent.content
    return None


def chunk_text(
    text: str,
    chunker: "BaseChunker",  # noqa: F821
    sep_pattern: str | None = _PARAGRAPH_SEP,
) -> Iterator["BaseChunk"]:  # noqa: F821
    from docling_core.types.doc import DocItemLabel, DoclingDocument  # noqa: PLC0415

    dl_doc = DoclingDocument(name="content")
    if sep_pattern is not None:
        pattern = _compile_pattern(sep_pattern)
        paragraphs = (p.strip() for p in pattern.split(text))
    else:
        paragraphs = [text]
    for paragraph in paragraphs:
        if paragraph:
            dl_doc.add_text(label=DocItemLabel.TEXT, text=paragraph)
    yield from chunker.chunk(dl_doc)


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


async def with_embeddable_content(
    batches: AsyncIterable[list[Document]],
    *,
    es_client: ESClient,
    project: str,
    artifacts_root: Path,
) -> AsyncIterable[list[Document]]:
    """Fall back to doc context when no markdown present (skip if neither available).

    :param batches: batches of documents to filter
    :param es_client: ES client
    :param project: document project
    :param artifacts_root: root of the artifacts directory
    :return: batches of embeddable documents
    """
    async for batch in batches:
        missing = [d.id for d in batch if not _has_structure(d, artifacts_root)]
        contents = await _fetch_contents(es_client, project, missing)
        embeddable = []
        for d in batch:
            if d.id not in contents:
                embeddable.append(d)
                continue
            content = contents[d.id]
            if content is None or not content.strip():
                logger.debug("skipping %s (no markdown/content)", d.id)
                continue
            embeddable.append(d.model_copy(update={"content": content}))
        if embeddable:
            yield embeddable


def _has_structure(doc: Document, artifacts_root: Path) -> bool:
    structure_dir = artifact_path(
        doc.id, StructureArtifact, project=doc.project, root=artifacts_root
    )
    return _structure_md_path(structure_dir) is not None


async def _fetch_contents(
    es_client: ESClient, project: str, doc_ids: list[DocId]
) -> dict[DocId, str | None]:
    contents = dict.fromkeys(doc_ids)
    if not doc_ids:
        return contents
    body = {QUERY: has_id(doc_ids), "size": len(doc_ids)}
    res = await es_client.search(
        index=project, body=body, _source_includes=[DOC_CONTENT]
    )
    for hit in res[HITS][HITS]:
        contents[hit[ID_]] = hit[SOURCE].get(DOC_CONTENT)
    return contents


@cache
def _compile_pattern(p: str) -> re.Pattern:
    return re.compile(p)
