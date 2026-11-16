import random
from collections.abc import AsyncIterator, Iterable

import pytest
from datashare_python.objects import DatashareLanguage, Document
from docling_core.transforms.chunker import BaseChunk, BaseChunker
from icij_common.es import ESClient

from extract_worker.config import EmbeddingWorkerConfig
from extract_worker.embeddings import (
    HFEmbedder,
    chunk_text,
    embed_doc_chunks,
    index_chunks,
)
from extract_worker.search import (
    ESRetriever,
    ESRetrieverConfig,
    LikeDoc,
    LikeText,
    Retriever,
    SimilarDocSearch,
    SimilarityType,
    UnlikeDoc,
    UnlikeText,
    WithoutTerms,
    WithTerms,
)

from . import TEST_RESOURCE_PATH
from .conftest import TEST_EMBEDDING_DIM, TEST_PROJECT, index_docs

_TEST_SEP_PATTERN = "sep"


@pytest.fixture(scope="session")
def source_doc() -> Document:
    return Document(
        id="source",
        index=TEST_PROJECT,
        content="what a sunny and beautiful day !",
        language=DatashareLanguage("ENGLISH"),
    )


@pytest.fixture(scope="session")
def similar_semantics() -> Document:
    return Document(
        id="similar-semantics",
        index=TEST_PROJECT,
        content="what a nice weather, the sky is blue and the sun shines",
        language=DatashareLanguage("ENGLISH"),
    )


@pytest.fixture(scope="session")
def similar_terms() -> Document:
    return Document(
        id="similar-terms",
        index=TEST_PROJECT,
        content="Sunny, what is a beautiful first name !",
        language=DatashareLanguage("ENGLISH"),
    )


@pytest.fixture(scope="session")
def random_doc() -> Document:
    return Document(
        id="random-doc",
        index=TEST_PROJECT,
        content="i'm convey no special meaning nor keyword terms",
        language=DatashareLanguage("ENGLISH"),
    )


@pytest.fixture(scope="session")
def similar_chunk_but_noisy(words: list[str]) -> Document:
    random.seed(66)
    noisy_content = " ".join(random.choices(words, k=100))
    noisy_content += f" {_TEST_SEP_PATTERN} "
    noisy_content += "what sunny beautiful day"
    noisy_content += f" {_TEST_SEP_PATTERN} "
    noisy_content += " ".join(random.choices(words, k=100))
    return Document(
        id="similar-chunk-but-noisy",
        index=TEST_PROJECT,
        content=noisy_content,
        language=DatashareLanguage("ENGLISH"),
    )


@pytest.fixture(scope="session")
def words() -> list[str]:
    words_path = TEST_RESOURCE_PATH / "wordlist.txt"
    words = (line.strip() for line in words_path.read_text().splitlines())
    words = [word for word in words if word]
    return words


def _single_page_chunks(
    text: str, chunker: BaseChunker
) -> Iterable[tuple[int, BaseChunk]]:
    for c in chunk_text(text, chunker, sep_pattern=_TEST_SEP_PATTERN):
        yield 0, c


@pytest.fixture
async def populate_similar_docs(  # noqa: PLR0917
    test_es_client: ESClient,
    test_embedder: HFEmbedder,
    test_chunker: BaseChunker,
    source_doc: Document,
    similar_semantics: Document,
    similar_terms: Document,
    random_doc: Document,
) -> list[Document]:
    docs = [source_doc, similar_semantics, similar_terms, random_doc]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    await _embed_docs(docs, test_chunker, test_embedder, test_es_client)
    return docs


@pytest.fixture
async def populate_noisy_similar_docs(  # noqa: PLR0917
    test_es_client: ESClient,
    test_embedder: HFEmbedder,
    test_chunker: BaseChunker,
    source_doc: Document,
    similar_semantics: Document,
    similar_chunk_but_noisy: Document,
) -> list[Document]:
    docs = [source_doc, similar_semantics, similar_chunk_but_noisy]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    await _embed_docs(docs, test_chunker, test_embedder, test_es_client)
    return docs


async def _async_iter[I](it: Iterable[I]) -> AsyncIterator[I]:
    for item in it:
        yield item


async def _embed_docs(
    docs: list[Document],
    test_chunker: BaseChunker,
    test_embedder: HFEmbedder,
    test_es_client: ESClient,
) -> None:
    index = test_embedder.config.index_name(TEST_PROJECT)
    per_page_chunks = (_single_page_chunks(doc.content, test_chunker) for doc in docs)
    chunks = []
    for doc, page_chunks in zip(docs, per_page_chunks, strict=True):
        per_page_chunks = list(page_chunks)  # noqa: PLW2901
        embedded_chunks = await embed_doc_chunks(
            doc,
            per_page_chunks,
            chunker=test_chunker,
            embedder=test_embedder,
            dim=TEST_EMBEDDING_DIM,
        )
        chunks += embedded_chunks
    chunks = _async_iter(chunks)
    await index_chunks(
        chunks, test_es_client, index, dict(), dim=TEST_EMBEDDING_DIM, bulk_size=20
    )


@pytest.fixture(scope="session")
async def retriever(
    test_es_client_session: ESClient,
    test_embedder: HFEmbedder,
    test_chunker: BaseChunker,
) -> Retriever:
    embedding_config = EmbeddingWorkerConfig()
    chunk_index = embedding_config.index_name(TEST_PROJECT)
    config = ESRetrieverConfig(
        index=TEST_PROJECT, chunks_index=chunk_index, dim=TEST_EMBEDDING_DIM
    )
    retriever = ESRetriever(test_es_client_session, test_embedder, test_chunker, config)
    return retriever


@pytest.mark.parametrize(
    ("search", "expected_docs", "top_k"),
    [
        # LikeDoc, term similarity
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY)
                ]
            ),
            ["similar-terms", "similar-semantics"],
            3,
        ),
        # LikeDoc, semantic similarity
        (
            SimilarDocSearch(
                like=[LikeDoc(doc_id="source", similarity=SimilarityType.EMBEDDING)]
            ),
            ["similar-semantics", "similar-terms", "random-doc"],
            3,
        ),
        # LikeDoc, hybrid, text should win
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(
                        doc_id="source",
                        similarity=SimilarityType.HYBRID,
                        term_frequency_weight=10,
                    )
                ]
            ),
            ["similar-terms", "similar-semantics", "random-doc"],
            3,
        ),
        # LikeDoc, hybrid, embedding should win
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(
                        doc_id="source",
                        similarity=SimilarityType.HYBRID,
                        embedding_weight=100,
                    )
                ]
            ),
            ["similar-semantics", "similar-terms", "random-doc"],
            3,
        ),
        # LikeText, text similarity
        (
            SimilarDocSearch(
                like=[
                    LikeText(
                        text="beautiful first name",
                        similarity=SimilarityType.TERM_FREQUENCY,
                    )
                ]
            ),
            ["similar-terms", "source"],
            3,
        ),
        # LikeText, semantic similarity
        (
            SimilarDocSearch(
                like=[
                    LikeText(
                        text="it feels like summer today, we have perfect conditions",
                        similarity=SimilarityType.EMBEDDING,
                    )
                ]
            ),
            ["source", "similar-semantics", "similar-terms"],
            3,
        ),
        # LikeText, hybrid text should win
        (
            SimilarDocSearch(
                like=[
                    LikeText(
                        text="beautiful summer day",
                        similarity=SimilarityType.HYBRID,
                        term_frequency_weight=10,
                    )
                ]
            ),
            ["source", "similar-terms", "similar-semantics"],
            3,
        ),
        # LikeText, hybrid embedding should win
        (
            SimilarDocSearch(
                like=[
                    LikeText(
                        text="beautiful summer day",
                        similarity=SimilarityType.HYBRID,
                        embedding_weight=10,
                    )
                ]
            ),
            ["source", "similar-semantics", "similar-terms"],
            3,
        ),
        # WithTerms
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY),
                    WithTerms(terms=["first"]),
                ]
            ),
            ["similar-terms"],
            3,
        ),
        # UnlikeDoc
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY)
                ],
                unlike=[UnlikeDoc(doc_id="similar-terms")],
            ),
            ["similar-semantics"],
            3,
        ),
        # UnlikeText
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY)
                ],
                unlike=[
                    UnlikeText(
                        text="Sunny, what is a beautiful first name !", weight=10
                    )
                ],
            ),
            ["similar-semantics", "similar-terms"],
            3,
        ),
        # WithoutTerms
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY)
                ],
                unlike=[WithoutTerms(terms=["sunny", "beautiful", "weather"])],
            ),
            [],
            3,
        ),
        # LikeDoc, LikeText, doc should win
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(
                        doc_id="source",
                        similarity=SimilarityType.EMBEDDING,
                        embedding_weight=10,
                    ),
                    LikeText(
                        text="beautiful first name",
                        similarity=SimilarityType.TERM_FREQUENCY,
                    ),
                ]
            ),
            ["similar-semantics", "similar-terms"],
            2,
        ),
        # LikeDoc, LikeText, text should win
        (
            SimilarDocSearch(
                like=[
                    LikeDoc(
                        doc_id="source",
                        similarity=SimilarityType.EMBEDDING,
                    ),
                    LikeText(
                        text="beautiful first name",
                        similarity=SimilarityType.TERM_FREQUENCY,
                        term_frequency_weight=10,
                    ),
                ]
            ),
            ["similar-terms", "similar-semantics"],
            2,
        ),
    ],
)
@pytest.mark.usefixtures("populate_similar_docs")
async def test_es_retriever(
    retriever: ESRetriever,
    search: SimilarDocSearch,
    top_k: int,
    expected_docs: list[str],
) -> None:
    # When
    async with retriever:
        docs = await retriever.retrieve_similar(search, top_k=top_k)
    doc = [d.id for d in docs]
    # Then
    assert doc == expected_docs


@pytest.mark.usefixtures("populate_noisy_similar_docs")
async def test_es_retriever_should_use_chunking_to_filter_out_noise(
    retriever: ESRetriever,
) -> None:
    # Given
    top_k = 1
    search = SimilarDocSearch(
        like=[LikeDoc(doc_id="source", similarity=SimilarityType.TERM_FREQUENCY)]
    )
    # When
    async with retriever:
        docs = await retriever.retrieve_similar(search, top_k=top_k)
    doc = [d.id for d in docs]
    # Then
    expected_docs = ["similar-chunk-but-noisy"]
    assert doc == expected_docs
