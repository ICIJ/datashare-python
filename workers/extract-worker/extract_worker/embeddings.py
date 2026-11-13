import asyncio
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
)
from functools import cache
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

from aiofile import async_open
from datashare_python.exceptions import ArtifactNotFound
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    DatashareFile,
    Document,
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
    WorkerFile,
    WorkerPaths,
)
from datashare_python.types_ import AsyncProgressRateHandler
from datashare_python.utils import (
    artifact_path,
    async_has_artifact,
    async_read_jsonl_as,
    paginate_md,
    reports_errors,
    to_raw_async_progress,
)
from elasticsearch.helpers import async_bulk
from icij_common.es import (
    ASC,
    DOC_CONTENT,
    DOC_CONTENT_TYPE,
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

from extract_worker.constants import CHUNK_DOC_ID, CHUNK_EMBEDDING, CHUNK_INDEX

from .config import EmbeddingWorkerConfig
from .objects import (
    DocId,
    DocumentSearchQuery,
    EmbeddedChunk,
    EmbeddingError,
    EmbeddingResponse,
    StructureArtifact,
    Vector,
)

if TYPE_CHECKING:
    from docling.document_converter import DocumentConverter
    from docling_core.transforms.chunker import BaseChunker
    from docling_core.transforms.chunker.base import BaseChunk


logger = logging.getLogger(__name__)

_DELETE_GROUP_SIZE = 256
_PARAGRAPH_SEP = r"\n\s*\n"

# TODO: put specific errors in here
_EMBEDDING_RECOVERABLES = (ArtifactNotFound,)


class MissingEmbeddingsIndex(ValueError):
    def __init__(self, index: str) -> None:
        super().__init__(f"embeddings index {index} doesn't exist, create it first")
        self.index = index


class EmbeddingSizeMismatch(ValueError):
    def __init__(self, index: str, expected: int, actual: int | None) -> None:
        if actual is None:
            msg = f"embeddings index {index} has no {CHUNK_EMBEDDING} vector field"
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
    async def embed(self, texts: list[str], dim: int) -> list[Vector]: ...


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
        trust_remote_code: bool = False,
    ) -> None:
        super().__init__(config)
        import torch  # noqa: PLC0415
        from transformers import AutoModel  # noqa: PLC0415

        self.__model = model
        self._task = config.task
        self._batch_size = config.inference_batch_size
        device = config.device.value
        dtype = torch.bfloat16 if device == "cuda" else torch.float32
        if model_revision is None:
            if trust_remote_code:
                raise ValueError("model revision must be provided to trust remote code")
        else:
            trust_remote_code = True
        self._model = AutoModel.from_pretrained(
            model,
            revision=model_revision,
            trust_remote_code=trust_remote_code,
            dtype=dtype,
        ).to(device)

    @property
    def model(self) -> str:
        return self.__model

    async def embed(self, texts: list[str], dim: int) -> list[Vector]:
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
    config: EmbeddingWorkerConfig,
    *,
    model: str,
    model_revision: str | None = None,
    merge_peers: bool,
) -> "HybridChunker":  # noqa: F821
    from docling_core.transforms.chunker.hybrid_chunker import (  # noqa: PLC0415
        HybridChunker,
    )
    from docling_core.transforms.chunker.tokenizer.huggingface import (  # noqa: PLC0415
        HuggingFaceTokenizer,
    )

    trust_remote_code = model_revision is not None
    # use the embedding model tokenizer so that chunks fit the model context
    tokenizer = HuggingFaceTokenizer.from_pretrained(
        model,
        max_tokens=config.max_tokens,
        revision=model_revision,
        trust_remote_code=trust_remote_code,
    )
    return HybridChunker(tokenizer=tokenizer, merge_peers=merge_peers)


def make_md_converter() -> "DocumentConverter":  # noqa: F821
    from docling.datamodel.base_models import InputFormat  # noqa: PLC0415
    from docling.document_converter import DocumentConverter  # noqa: PLC0415

    return DocumentConverter(allowed_formats=[InputFormat.MD])


def load_embedding_resources(
    config: EmbeddingWorkerConfig, model: str, model_revision: str | None
) -> tuple["BaseChunker", "DocumentConverter", HFEmbedder]:
    logger.debug("loading embedding model %s (revision %s)...", model, model_revision)
    resources = (
        make_chunker(
            config, model=model, model_revision=model_revision, merge_peers=False
        ),
        make_md_converter(),
        HFEmbedder(config, model=model, model_revision=model_revision),
    )
    logger.debug("embedding model loaded !")
    return resources


async def embed_documents_act(
    batch: Path,
    *,
    chunker: "BaseChunker",
    md_converter: "DocumentConverter",
    embedder: Embedder,
    dim: int,
    paths: WorkerPaths,
    output_path: Path,
    progress: AsyncProgressRateHandler | None = None,
) -> tuple[EmbeddingResponse, dict[DocId, int]]:
    docs = [d async for d in async_read_jsonl_as(batch, PROCESSED_FILE_TA)]
    n_docs = len(docs)
    if progress is not None:
        progress = to_raw_async_progress(progress, max_progress=n_docs)
    n_pages, n_successes, n_successes_pages = 0, 0, 0
    errors = []
    n_chunks_by_doc = dict()
    doc_chunks = embed_docs(docs, chunker, md_converter, embedder, dim, paths=paths)
    doc_i = 0
    async with async_open(output_path, "w") as f:
        async for res in doc_chunks:
            if isinstance(res, EmbeddingError):
                errors.append(res)
                n_pages += res.source.n_pages
            else:
                doc, chunks = res
                for chunk in chunks:
                    await f.write(f"{chunk.model_dump_json()}\n")
                n_chunks_by_doc[doc.doc_id] = len(chunks)
                n_pages += doc.n_pages
                n_successes += 1
                n_successes_pages += doc.n_pages
            doc_i += 1
            if progress is not None:
                await progress(doc_i)
    processed = ProcessingReportWithPages(n_docs=n_docs, n_pages=n_pages)
    successes = ProcessingReportWithPages(n_docs=n_successes, n_pages=n_successes_pages)
    errors = ErrorReportWithPages.from_errors(*errors)
    n_chunks = sum(n_chunks_by_doc.values())
    response = EmbeddingResponse(
        processed=processed, successes=successes, errors=errors, n_chunks=n_chunks
    )
    return response, n_chunks_by_doc


async def embed_docs(
    docs: Iterable[ProcessedFile],
    chunker: "BaseChunker",
    md_converter: "DocumentConverter",
    embedder: Embedder,
    dim: int,
    *,
    paths: WorkerPaths,
) -> AsyncGenerator[tuple[ProcessedFile, list[EmbeddedChunk]] | EmbeddingError]:
    for doc in docs:
        res = embed_doc(doc, chunker, md_converter, embedder, dim=dim, paths=paths)
        yield await res


@reports_errors(_EMBEDDING_RECOVERABLES, EmbeddingError)
async def embed_doc(
    doc: ProcessedFile,
    chunker: "BaseChunker",
    md_converter: "DocumentConverter",
    embedder: Embedder,
    dim: int,
    *,
    paths: WorkerPaths,
) -> tuple[ProcessedFile, list[EmbeddedChunk]]:
    chunks = [c async for c in page_chunks(doc, chunker, md_converter, paths)]
    if not chunks:
        return doc, []
    res = await embed_doc_chunks(
        doc, chunks, chunker=chunker, embedder=embedder, dim=dim
    )
    return doc, res


async def index_embeddings_act(
    chunks_path: Path,
    n_chunks_by_doc: dict[DocId, int],
    *,
    es_client: ESClient,
    index: str,
    dim: int,
    bulk_size: int,
) -> int:
    chunks = (c async for c in async_read_jsonl_as(chunks_path, EmbeddedChunk))
    n_indexed = await index_chunks(
        chunks, es_client, index, n_chunks_by_doc, dim=dim, bulk_size=bulk_size
    )
    logger.debug(
        "indexed %s chunks for %s docs in %s", n_indexed, len(n_chunks_by_doc), index
    )
    return n_indexed


async def index_chunks(
    chunks: AsyncIterable[EmbeddedChunk],
    es_client: ESClient,
    index: str,
    n_chunks_by_doc: dict[str, int],
    *,
    dim: int,
    bulk_size: int,
) -> int:
    await ensure_embeddings_index(es_client, index, dim=dim)
    actions = (_index_action(c, index) async for c in chunks)
    n_indexed, _ = await async_bulk(
        es_client,
        actions,
        chunk_size=bulk_size,
        stats_only=True,
        raise_on_error=True,
        refresh="wait_for",
    )
    await _delete_stale_chunks(es_client, index, n_chunks_by_doc)
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
                        {"term": {CHUNK_DOC_ID: doc_id}},
                        {"range": {CHUNK_INDEX: {"gte": n_chunks}}},
                    ]
                }
            }
            for doc_id, n_chunks in group
        ]
        query = {"bool": {"should": stale, "minimum_should_match": 1}}
        await es_client.delete_by_query(
            index=index, body={QUERY: query}, conflicts="proceed", refresh=True
        )


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
    actual = properties.get(CHUNK_EMBEDDING, dict()).get("dims")
    if actual != dim:
        raise EmbeddingSizeMismatch(index, expected=dim, actual=actual)


def _index_action(chunk: EmbeddedChunk, index: str) -> dict[str, Any]:
    return {
        "_op_type": "index",
        "_index": index,
        "_routing": chunk.root_document,
        ID_: chunk.id,
        "_source": chunk.model_dump(by_alias=True),
    }


async def embed_doc_chunks(
    doc: ProcessedFile,
    chunks_by_page: list[tuple[int, "BaseChunk"]],
    *,
    chunker: "BaseChunker",
    embedder: Embedder,
    dim: int,
) -> list[EmbeddedChunk]:
    # embed chunks with their headings for context, but only store the chunk text
    texts = [chunker.contextualize(chunk) for _, chunk in chunks_by_page]
    embeddings = await embedder.embed(texts, dim)
    return [
        EmbeddedChunk(
            doc_id=doc.doc_id,
            root_document=doc.root_document,
            chunk_index=chunk_i,
            page=page,
            text=chunk.text,
            headings=chunk.meta.headings,
            model=embedder.model,
            embedding=embedding,
        )
        for chunk_i, ((page, chunk), embedding) in enumerate(
            zip(chunks_by_page, embeddings, strict=True)
        )
    ]


async def page_chunks(
    doc: ProcessedFile,
    chunker: "BaseChunker",
    md_converter: "DocumentConverter",
    paths: WorkerPaths,
) -> AsyncIterator[tuple[int, "BaseChunk"]]:
    if isinstance(doc, WorkerFile):
        content = await _text_content(doc, paths)
        for chunk in chunk_text(content, chunker):
            yield 1, chunk
        return
    async for page, md in _read_structure_pages(doc, paths):
        for chunk in _chunk_markdown(md, chunker, md_converter):
            yield page, chunk


async def _text_content(doc: ProcessedFile, paths: WorkerPaths) -> str:
    fallback_content_path = doc.locate(paths)
    async with async_open(fallback_content_path) as f:
        return await f.read()


def chunk_text(
    text: str,
    chunker: "BaseChunker",
    sep_pattern: str | None = _PARAGRAPH_SEP,
) -> Iterator["BaseChunk"]:
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
    md: bytes, chunker: "BaseChunker", md_converter: "DocumentConverter"
) -> Iterator["BaseChunk"]:
    from docling.datamodel.base_models import DocumentStream  # noqa: PLC0415

    if not md.strip():
        return
    stream = DocumentStream(name="page.md", stream=BytesIO(md))
    dl_doc = md_converter.convert(stream).document
    yield from chunker.chunk(dl_doc)


async def _read_structure_pages(
    doc: ProcessedFile, paths: WorkerPaths
) -> AsyncGenerator[tuple[int, bytes]]:
    current_page = 1
    async for p in paginate_md(paths, doc_id=doc.doc_id, project=doc.project):
        yield current_page, p
        current_page += 1


def _structure_md_path(structure_dir: Path) -> Path | None:
    if not structure_dir.is_dir():
        return None
    return structure_dir


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


async def locate_embeddable_content(
    batches: AsyncIterable[list[Document]],
    output_dir: Path,
    *,
    es_client: ESClient,
    project: str,
    paths: WorkerPaths,
) -> AsyncIterable[list[ProcessedFile]]:
    async for batch in batches:
        md_paths = {d.id: await _md_structure_path(d, paths) for d in batch}
        missing = [d for d, path in md_paths.items() if path is None]
        contents = dict()
        if missing:
            contents = await _fetch_contents(es_client, project, missing)
        embeddable = []
        for d in batch:
            ds_file = DatashareFile.from_parent(d)
            if md_paths[d.id] is not None:
                embeddable.append(ds_file)
                continue
            content = contents.get(d.id)
            if content is None or not content.strip():
                logger.debug("skipping %s (no markdown/content)", d.id)
                continue
            content_path = output_dir / f"{d.id}-content"
            async with async_open(content_path, "w") as f:
                await f.write(content)
            embeddable.append(WorkerFile.from_parent(ds_file, content_path, paths))
        if embeddable:
            yield embeddable


async def _md_structure_path(doc: Document, paths: WorkerPaths) -> Path | None:
    has_structure = await async_has_artifact(
        paths, doc.id, StructureArtifact, project=doc.project
    )
    if not has_structure:
        return None
    structure_dir = artifact_path(
        doc.id, StructureArtifact, project=doc.project, root=paths.artifacts
    )
    return structure_dir


_DOC_SORT = [{DOC_CONTENT_TYPE: ASC}, {DOC_LANGUAGE: ASC}, {"_doc": ASC}]


async def _fetch_contents(
    es_client: ESClient, project: str, doc_ids: list[DocId]
) -> dict[DocId, str | None]:
    contents = dict()
    body = {QUERY: has_id(doc_ids)}
    async for res in es_client.poll_search_pages(
        index=project, body=body, source=[DOC_CONTENT], sort=_DOC_SORT
    ):
        for hit in res[HITS][HITS]:
            contents[hit[ID_]] = hit[SOURCE].get(DOC_CONTENT)
    return contents


@cache
def _compile_pattern(p: str) -> re.Pattern:
    return re.compile(p)
