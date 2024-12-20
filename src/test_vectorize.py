# pylint: disable=redefined-outer-name
# --8<-- [start:test-vectorize]
from pathlib import Path
from typing import List

import pytest
from icij_common.es import ESClient
from lancedb import AsyncConnection as LanceDBConnection, connect_async

from datashare_python.objects import Document
from datashare_python.tasks.vectorize import (
    create_vectorization_tasks,
    find_most_similar,
    make_record_schema,
    recreate_vector_table,
    vectorize_docs,
)
from datashare_python.tests.conftest import TEST_PROJECT
from datashare_python.utils import DSTaskClient


@pytest.fixture
async def test_vector_db(tmpdir) -> LanceDBConnection:
    db = await connect_async(Path(tmpdir) / "test_vectors.db")
    return db


@pytest.mark.integration
async def test_create_vectorization_tasks(
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_es_client: ESClient,
    test_task_client: DSTaskClient,
    test_vector_db: LanceDBConnection,
):
    # When
    task_ids = await create_vectorization_tasks(
        project=TEST_PROJECT,
        es_client=test_es_client,
        task_client=test_task_client,
        vector_db=test_vector_db,
        batch_size=2,
    )
    # Then
    assert len(task_ids) == 2


@pytest.mark.integration
async def test_vectorize_docs(
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_es_client: ESClient,
    test_vector_db: LanceDBConnection,
):
    # Given
    model = "BAAI/bge-small-en-v1.5"
    docs = ["doc-0", "doc-3"]
    schema = make_record_schema(model)
    await recreate_vector_table(test_vector_db, schema)

    # When
    n_vectorized = await vectorize_docs(
        docs,
        TEST_PROJECT,
        es_client=test_es_client,
        vector_db=test_vector_db,
    )
    # Then
    assert n_vectorized == 2
    table = await test_vector_db.open_table("ds_docs")
    records = await table.query().to_list()
    assert len(records) == 2
    doc_ids = sorted(d["doc_id"] for d in records)
    assert doc_ids == ["doc-0", "doc-3"]
    assert all("vector" in r for r in records)


@pytest.mark.integration
async def test_find_most_similar(test_vector_db: LanceDBConnection):
    # Given
    model = "BAAI/bge-small-en-v1.5"
    schema = make_record_schema(model)
    table = await recreate_vector_table(test_vector_db, schema)
    docs = [
        {"doc_id": "novel", "content": "I'm a doc about novels"},
        {"doc_id": "monkey", "content": "I'm speaking about monkeys"},
    ]
    await table.add(docs)
    queries = ["doc about books", "doc speaking about animal"]

    # When
    n_similar = 1
    most_similar = await find_most_similar(
        queries, model, vector_db=test_vector_db, n_similar=n_similar
    )
    # Then
    assert len(most_similar) == 2
    similar_ids = [s["doc_id"] for s in most_similar]
    assert similar_ids == ["novel", "monkey"]
    assert all("distance" in s for s in most_similar)


# --8<-- [end:test-vectorize]
