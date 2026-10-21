from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    DatashareFile,
    Document,
    ProcessedFile,
    ProcessingReportWithPages,
)
from datashare_python.utils import read_jsonl_as
from docling_core.transforms.chunker.hierarchical_chunker import HierarchicalChunker
from extract_worker.activities import create_embedding_batches_act
from extract_worker.config import ExtractWorkerConfig
from extract_worker.embeddings import (
    build_embedding_doc_query,
    embed_documents_act,
    embeddings_index_body,
    index_embeddings_act,
    make_md_converter,
    read_structure_pages,
)
from extract_worker.objects import (
    DocId,
    DocumentSearchQuery,
    EmbeddedChunk,
)
from icij_common.es import HITS, ESClient

from tests import PAGE_SEP, PAGES

_DIM = 4


class _FakeEmbedder:
    model = "fake-model"

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(t))] * _DIM for t in texts]


def test_read_structure_pages(
    doc_with_structure: ProcessedFile, test_worker_config: ExtractWorkerConfig
) -> None:
    # When
    pages = list(
        read_structure_pages(doc_with_structure, test_worker_config.paths.artifacts)
    )

    # Then
    expected = [(0, (PAGES[0] + PAGE_SEP).encode()), (1, PAGES[1].encode())]
    assert pages == expected


def test_read_structure_pages_should_raise_for_missing_structure(
    doc_2: Document, test_worker_config: ExtractWorkerConfig
) -> None:
    # Given
    doc = DatashareFile.from_parent(doc_2)

    # When/Then
    with pytest.raises(FileNotFoundError):
        list(read_structure_pages(doc, test_worker_config.paths.artifacts))


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
    response = await embed_documents_act(
        batch,
        chunker=HierarchicalChunker(),
        md_converter=make_md_converter(),
        embedder=_FakeEmbedder(),
        artifacts_root=test_worker_config.paths.artifacts,
        output_path=output_path,
    )

    # Then
    assert response.processed == ProcessingReportWithPages(n_docs=2, n_pages=3)
    assert response.successes == ProcessingReportWithPages(n_docs=1, n_pages=2)
    assert response.errors.n_docs == 1
    assert response.errors.errors[0].doc_id == missing.doc_id
    assert response.n_chunks == 2
    chunks = list(read_jsonl_as(output_path, EmbeddedChunk))
    assert [c.chunk_index for c in chunks] == [0, 1]
    assert [c.page for c in chunks] == [0, 1]
    assert all(c.doc_id == doc_with_structure.doc_id for c in chunks)
    assert all(c.root_document == "root-0" for c in chunks)
    assert chunks[0].headings == ["Offshore leaks"]
    assert "Shell companies" in chunks[0].text
    assert chunks[1].headings == ["Beneficial owners"]
    assert all(len(c.embedding) == _DIM for c in chunks)
    assert all(c.model == "fake-model" for c in chunks)


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
async def test_create_embedding_batches_act(
    populate_es: list[Document],  # noqa: ARG001
    doc_with_structure: ProcessedFile,
    test_es_client: ESClient,
    test_worker_config: ExtractWorkerConfig,
    tmp_path: Path,
) -> None:
    # When
    batches = [
        b
        async for b in create_embedding_batches_act(
            None,
            TEST_PROJECT,
            artifacts_root=test_worker_config.paths.artifacts,
            output_dir=tmp_path,
            target_n_docs_per_task=10,
            es_client=test_es_client,
        )
    ]

    # Then
    assert len(batches) == 1
    docs = list(read_jsonl_as(batches[0], PROCESSED_FILE_TA))
    assert [d.doc_id for d in docs] == [doc_with_structure.doc_id]


def _chunk(doc_id: str, chunk_index: int) -> EmbeddedChunk:
    return EmbeddedChunk(
        doc_id=doc_id,
        root_document=doc_id,
        project=TEST_PROJECT,
        chunk_index=chunk_index,
        page=0,
        text=f"chunk {chunk_index}",
        model="fake-model",
        embedding=[0.5] * _DIM,
    )


@pytest.mark.integration
async def test_index_embeddings_act(test_es_client: ESClient, tmp_path: Path) -> None:
    # Given
    index = f"{TEST_PROJECT}-embeddings"
    stale = tmp_path / "stale.jsonl"
    stale.write_text("\n".join(_chunk("doc-0", i).model_dump_json() for i in range(3)))
    chunks_path = tmp_path / "chunks.jsonl"
    chunks = [_chunk("doc-0", 0), _chunk("doc-0", 1), _chunk("doc-1", 0)]
    chunks_path.write_text("\n".join(c.model_dump_json() for c in chunks))
    kwargs = {"es_client": test_es_client, "index": index, "dim": _DIM}

    # When
    await index_embeddings_act(stale, bulk_size=2, **kwargs)
    n_indexed = await index_embeddings_act(chunks_path, bulk_size=2, **kwargs)

    # Then
    assert n_indexed == 3
    mapping = await test_es_client.indices.get_mapping(index=index)
    expected_mapping = embeddings_index_body(_DIM)["mappings"]
    assert mapping[index]["mappings"] == expected_mapping
    res = await test_es_client.search(
        index=index, body={"query": {"match_all": {}}, "sort": ["_id"]}
    )
    hits = res[HITS][HITS]
    # doc-0 stale chunk 2 should have been removed
    assert [h["_id"] for h in hits] == ["doc-0-0", "doc-0-1", "doc-1-0"]
    assert hits[0]["_routing"] == "doc-0"
    assert hits[0]["_source"] == chunks[0].model_dump(by_alias=True)
