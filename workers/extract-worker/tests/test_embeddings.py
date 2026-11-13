import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from conftest import DOC_0_PAGES, DOC_3_CONTENT, PAGE_SEP
from datashare_python.conftest import ES_SETTINGS, TEST_PROJECT
from datashare_python.exceptions import ArtifactNotFound
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    DatashareFile,
    Document,
    ProcessedFile,
    ProcessingReportWithPages,
)
from datashare_python.utils import read_jsonl_as
from docling_core.transforms.chunker.hierarchical_chunker import HierarchicalChunker
from elasticsearch import BadRequestError, NotFoundError
from elasticsearch.helpers import async_bulk
from icij_common.es import ASC, HITS, ID_, ESClient

from extract_worker.activities import create_embedding_batches_act
from extract_worker.config import ExtractWorkerConfig
from extract_worker.constants import (
    CHUNK_DOC_ID,
    CHUNK_EMBEDDING,
    CHUNK_HEADINGS,
    CHUNK_ID,
    CHUNK_INDEX,
    CHUNK_MODEL,
    CHUNK_PAGE,
    CHUNK_ROOT_DOCUMENT,
    CHUNK_TEXT,
)
from extract_worker.embeddings import (
    Embedder,
    EmbeddingSizeMismatch,
    MissingEmbeddingsIndex,
    _read_structure_pages,
    build_embedding_doc_query,
    embed_documents_act,
    ensure_embeddings_index,
    index_embeddings_act,
    make_md_converter,
)
from extract_worker.objects import DocId, DocumentSearchQuery, EmbeddedChunk, Vector

_DIM = 4


def embeddings_index_body(dim: int) -> dict[str, Any]:
    body = {
        "settings": ES_SETTINGS,
        "mappings": {
            "dynamic": "strict",
            "properties": {
                CHUNK_INDEX: {"type": "integer"},
                CHUNK_ID: {"type": "keyword"},
                CHUNK_DOC_ID: {"type": "keyword"},
                CHUNK_EMBEDDING: {
                    "type": "dense_vector",
                    "dims": dim,
                    "index": True,
                    "similarity": "cosine",
                },
                CHUNK_HEADINGS: {"type": "text"},
                CHUNK_MODEL: {"type": "keyword"},
                CHUNK_PAGE: {"type": "integer"},
                CHUNK_ROOT_DOCUMENT: {"type": "keyword"},
                CHUNK_TEXT: {
                    "type": "text",
                    "index_options": "offsets",
                    "analyzer": "folding",
                },
            },
        },
    }
    return body


@pytest.fixture
async def tiny_embedding_index(test_es_client: ESClient) -> None:
    index = f"{TEST_PROJECT}-embeddings"
    with contextlib.suppress(NotFoundError):
        await test_es_client.indices.delete(index=index)
    await test_es_client.indices.create(index=index, body=embeddings_index_body(_DIM))


class _FakeEmbedder(Embedder):
    model = "fake-model"

    def __init__(self, config: None = None) -> None:
        self._config = config

    async def embed(self, texts: list[str], dim: int) -> list[Vector]:
        return [[float(len(t))] * dim for t in texts]


async def test_read_structure_pages(
    doc_with_structure: ProcessedFile, test_worker_config: ExtractWorkerConfig
) -> None:
    # When
    pages = [
        p
        async for p in _read_structure_pages(
            doc_with_structure, test_worker_config.paths
        )
    ]

    # Then
    expected = [(1, (DOC_0_PAGES[0] + PAGE_SEP).encode()), (2, DOC_0_PAGES[1].encode())]
    assert pages == expected


async def test_read_structure_pages_should_raise_for_missing_structure(
    doc_2: Document, test_worker_config: ExtractWorkerConfig
) -> None:
    # Given
    doc = DatashareFile.from_parent(doc_2)

    # When/Then
    with pytest.raises(ArtifactNotFound):
        _ = [p async for p in _read_structure_pages(doc, test_worker_config.paths)]


async def test_embed_documents_act(
    doc_with_structure: ProcessedFile,
    doc_2: Document,
    test_worker_config: ExtractWorkerConfig,
    tmp_path: Path,
) -> None:
    # Given
    missing = DatashareFile.from_parent(doc_2)
    batch = tmp_path / "batch.jsonl"
    with batch.open("w") as f:
        for d in [doc_with_structure, missing]:
            f.write(PROCESSED_FILE_TA.dump_json(d).decode() + "\n")
    output_path = tmp_path / "chunks.jsonl"

    # When
    response, n_chunks_by_doc = await embed_documents_act(
        batch,
        chunker=HierarchicalChunker(),
        md_converter=make_md_converter(),
        embedder=_FakeEmbedder(),
        dim=_DIM,
        paths=test_worker_config.paths,
        output_path=output_path,
    )

    # Then
    assert response.processed == ProcessingReportWithPages(n_docs=2, n_pages=3)
    assert response.successes == ProcessingReportWithPages(n_docs=1, n_pages=2)
    assert response.errors.n_docs == 1
    assert response.errors.errors[0].doc_id == missing.doc_id
    assert response.n_chunks == 2
    assert n_chunks_by_doc == {doc_with_structure.doc_id: 2}
    chunks = list(read_jsonl_as(output_path, EmbeddedChunk))
    assert [c.chunk_index for c in chunks] == [0, 1]
    assert [c.page for c in chunks] == [1, 2]
    assert all(c.doc_id == doc_with_structure.doc_id for c in chunks)
    assert all(c.root_document == "root-0" for c in chunks)
    assert chunks[0].headings == ["Offshore leaks"]
    # stored text shouldn't contain the headings used as embedding context
    assert chunks[0].text == "Shell companies registered in the islands."
    assert chunks[1].headings == ["Beneficial owners"]
    assert all(len(c.embedding) == _DIM for c in chunks)
    assert all(c.model == "fake-model" for c in chunks)


@pytest.mark.usefixtures("tiny_embedding_index")
async def test_embed_documents_act_should_fallback_to_content(
    doc_without_md: Document, test_worker_config: ExtractWorkerConfig, tmp_path: Path
) -> None:
    # Given
    batch = tmp_path / "batch.jsonl"
    batch.write_text(PROCESSED_FILE_TA.dump_json(doc_without_md).decode() + "\n")
    output_path = tmp_path / "chunks.jsonl"

    # When
    response, n_chunks_by_doc = await embed_documents_act(
        batch,
        chunker=HierarchicalChunker(),
        md_converter=make_md_converter(),
        embedder=_FakeEmbedder(),
        dim=_DIM,
        paths=test_worker_config.paths,
        output_path=output_path,
    )

    # Then
    assert response.successes == ProcessingReportWithPages(n_docs=1, n_pages=1)
    assert response.errors.n_docs == 0
    assert n_chunks_by_doc == {doc_without_md.doc_id: 2}
    chunks = list(read_jsonl_as(output_path, EmbeddedChunk))
    expected_texts = ["The vote is on Sunday.", "Polls close at eight."]
    assert [c.text for c in chunks] == expected_texts
    assert [c.page for c in chunks] == [1, 1]
    assert all(c.headings is None for c in chunks)
    assert all(c.root_document == "root-3" for c in chunks)


@pytest.mark.parametrize(
    ("docs", "expected"),
    [
        (None, {"query": {"term": {"type": "Document"}}}),
        ({}, {"query": {"term": {"type": "Document"}}}),
        (
            ["a", "b"],
            {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"type": "Document"}},
                            {"ids": {"values": ["a", "b"]}},
                        ]
                    }
                }
            },
        ),
    ],
)
def test_build_embedding_doc_query(
    docs: list[DocId] | DocumentSearchQuery | None, expected: dict
) -> None:
    # When
    query = build_embedding_doc_query(docs)

    # Then
    assert query == expected


@pytest.mark.integration
async def test_create_embedding_batches_act(  # noqa: PLR0917
    populate_es: list[Document],  # noqa: ARG001
    doc_with_structure: ProcessedFile,
    doc_3: Document,
    test_es_client: ESClient,
    test_worker_config: ExtractWorkerConfig,
) -> None:
    # Given
    worker_paths = test_worker_config.paths
    paths = worker_paths
    output_dir = paths.workdir / "some-dir"
    # When
    batches = [
        b
        async for b in create_embedding_batches_act(
            None,
            TEST_PROJECT,
            paths=paths,
            output_dir=output_dir,
            target_n_docs_per_task=10,
            es_client=test_es_client,
        )
    ]

    # Then
    assert len(batches) == 1
    docs = list(read_jsonl_as(batches[0], PROCESSED_FILE_TA))
    by_id = {d.doc_id: d for d in docs}
    assert sorted(by_id) == [doc_with_structure.doc_id, doc_3.doc_id]
    assert by_id[doc_with_structure.doc_id].parent.content is None
    doc3_content_path = by_id[doc_3.doc_id].value.locate(worker_paths)
    assert doc3_content_path.read_text() == DOC_3_CONTENT


def _chunk(doc_id: str, chunk_index: int) -> EmbeddedChunk:
    return EmbeddedChunk(
        doc_id=doc_id,
        root_document=doc_id,
        chunk_index=chunk_index,
        page=0,
        text=f"chunk {chunk_index}",
        model="fake-model",
        embedding=[0.5] * _DIM,
    )


@pytest.mark.integration
@pytest.mark.usefixtures("tiny_embedding_index")
async def test_index_embeddings_act(test_es_client: ESClient, tmp_path: Path) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"
    stale = tmp_path / "stale.jsonl"
    stale.write_text("\n".join(_chunk("doc-0", i).model_dump_json() for i in range(3)))
    chunks_path = tmp_path / "chunks.jsonl"
    chunks = [_chunk("doc-0", 0), _chunk("doc-0", 1), _chunk("doc-1", 0)]
    chunks_path.write_text("\n".join(c.model_dump_json() for c in chunks))

    # When
    n_chunks_by_doc = {"doc-0": 3}
    await index_embeddings_act(
        stale,
        n_chunks_by_doc,
        bulk_size=2,
        es_client=test_es_client,
        dim=_DIM,
        index=index,
    )
    with patch("extract_worker.embeddings.async_bulk", wraps=async_bulk) as bulk:
        n_chunks_by_doc = {"doc-0": 2, "doc-1": 1}
        n_indexed = await index_embeddings_act(
            chunks_path,
            n_chunks_by_doc,
            bulk_size=2,
            es_client=test_es_client,
            dim=_DIM,
            index=index,
        )

    # Then
    assert n_indexed == 3
    bulk.assert_awaited_once()
    actions = bulk.await_args.args[1]
    assert hasattr(actions, "__aiter__")
    assert bulk.await_args.kwargs["chunk_size"] == 2
    indexed = await _indexed_ids(test_es_client, index)
    # doc-0 stale chunk 2 should have been removed
    assert indexed == ["doc-0-0", "doc-0-1", "doc-1-0"]
    res = await test_es_client.search(
        index=index, body={"query": {"match_all": {}}, "sort": [CHUNK_ID]}, size=1
    )
    hits = res[HITS][HITS]
    assert hits[0]["_routing"] == "doc-0"
    assert hits[0]["_source"] == chunks[0].model_dump(by_alias=True)


@pytest.mark.integration
async def test_index_embeddings_act_should_raise_for_missing_index(
    test_es_client: ESClient, tmp_path: Path
) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings-missing"
    chunks_path = tmp_path / "chunks.jsonl"
    chunks_path.write_text(_chunk("doc-0", 0).model_dump_json())

    # When/Then
    with pytest.raises(MissingEmbeddingsIndex):
        await index_embeddings_act(
            chunks_path,
            {"doc-0": 1},
            es_client=test_es_client,
            index=index,
            dim=_DIM,
            bulk_size=2,
        )
    assert not await test_es_client.indices.exists(index=index)


@pytest.mark.integration
@pytest.mark.usefixtures("tiny_embedding_index")
async def test_ensure_embeddings_index(test_es_client: ESClient) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"

    # When/Then
    await ensure_embeddings_index(test_es_client, index, dim=_DIM)


@pytest.mark.integration
@pytest.mark.usefixtures("tiny_embedding_index")
async def test_ensure_embeddings_index_should_raise_for_size_mismatch(
    test_es_client: ESClient,
) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"
    # When/Then
    with pytest.raises(EmbeddingSizeMismatch) as exc:
        await ensure_embeddings_index(test_es_client, index, dim=_DIM + 1)
    assert exc.value.expected == _DIM + 1
    assert exc.value.actual == _DIM


@pytest.mark.integration
async def test_ensure_embeddings_index_should_raise_for_missing_vector_field(
    test_es_client: ESClient,
) -> None:
    # Given
    index = f"{TEST_PROJECT}-missing-vector-field"
    body = {"mappings": {"properties": {"text": {"type": "text"}}}}
    with contextlib.suppress(BadRequestError):
        await test_es_client.indices.create(index=index, body=body)

    # When/Then
    with pytest.raises(EmbeddingSizeMismatch) as exc:
        await ensure_embeddings_index(test_es_client, index, dim=_DIM)
    assert exc.value.actual is None


async def _indexed_ids(es_client: ESClient, index: str) -> list[str]:
    body = {"query": {"match_all": {}}}
    sort = {CHUNK_ID: ASC}
    ids = []
    async for res in es_client.poll_search_pages(index=index, body=body, sort=sort):
        ids.extend(hit[ID_] for hit in res[HITS][HITS])
    return ids


@pytest.mark.integration
@pytest.mark.parametrize("delete_group_size", [1, 256])
@pytest.mark.usefixtures("tiny_embedding_index")
async def test_index_embeddings_act_should_only_replace_embedded_docs_chunks(
    test_es_client: ESClient, tmp_path: Path, delete_group_size: int
) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"
    previous = tmp_path / "previous.jsonl"
    previous_chunks = [_chunk("doc-0", 0), _chunk("doc-1", 0), _chunk("doc-2", 0)]
    previous.write_text("\n".join(c.model_dump_json() for c in previous_chunks))
    chunks = {"doc-0": 1, "doc-1": 1, "doc-2": 1}
    await index_embeddings_act(
        previous, chunks, es_client=test_es_client, dim=_DIM, bulk_size=10, index=index
    )
    chunks_path = tmp_path / "chunks.jsonl"
    chunks_path.write_text(_chunk("doc-0", 0).model_dump_json())

    # When
    with patch("extract_worker.embeddings._DELETE_GROUP_SIZE", delete_group_size):
        chunks = {"doc-0": 1, "doc-1": 0}
        await index_embeddings_act(
            chunks_path,
            chunks,
            es_client=test_es_client,
            dim=_DIM,
            bulk_size=10,
            index=index,
        )

    # Then
    assert await _indexed_ids(test_es_client, index) == ["doc-0-0", "doc-2-0"]


@pytest.mark.integration
@pytest.mark.usefixtures("tiny_embedding_index")
async def test_index_embeddings_act_should_keep_previous_chunks_on_write_failure(
    test_es_client: ESClient, tmp_path: Path
) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"
    previous = tmp_path / "previous.jsonl"
    previous.write_text(
        "\n".join(_chunk("doc-0", i).model_dump_json() for i in range(2))
    )
    chunks = {"doc-0": 2}
    await index_embeddings_act(
        previous, chunks, es_client=test_es_client, dim=_DIM, bulk_size=10, index=index
    )
    chunks_path = tmp_path / "chunks.jsonl"
    chunks_path.write_text(_chunk("doc-0", 0).model_dump_json())

    # When
    chunks = {"doc-0": 1}
    with (
        patch("extract_worker.embeddings.async_bulk", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await index_embeddings_act(
            chunks_path,
            chunks,
            bulk_size=10,
            es_client=test_es_client,
            dim=_DIM,
            index=index,
        )

    # Then
    assert await _indexed_ids(test_es_client, index) == ["doc-0-0", "doc-0-1"]
