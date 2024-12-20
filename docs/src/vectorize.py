# --8<-- [start:create_vectorization_tasks]
import asyncio
import logging
from collections.abc import AsyncIterable
from typing import AsyncIterator

import numpy as np
from icij_common.es import (
    DOC_CONTENT,
    ESClient,
    HITS,
    ID_,
    QUERY,
    SOURCE,
    ids_query,
    make_document_query,
    match_all,
)
from icij_worker.ds_task_client import DatashareTaskClient
from lancedb import AsyncConnection as LanceDBConnection, AsyncTable
from lancedb.embeddings import get_registry
from lancedb.index import FTS, IvfPq
from lancedb.pydantic import LanceModel, Vector
from sentence_transformers import SentenceTransformer

from datashare_python.constants import PYTHON_TASK_GROUP
from datashare_python.tasks.dependencies import (
    lifespan_es_client,
    lifespan_task_client,
    # --8<-- [start:lifespan_vector_db]
    lifespan_vector_db,
    # --8<-- [end:lifespan_vector_db]
)
from datashare_python.utils import async_batches

logger = logging.getLogger(__name__)


# --8<-- [start:create-table]
async def recreate_vector_table(
    vector_db: LanceDBConnection, schema: type[LanceModel]
) -> AsyncTable:
    table_name = "ds_docs"
    existing_tables = await vector_db.table_names()
    if table_name in existing_tables:
        logging.info("deleting previous vector db...")
        await vector_db.drop_table(table_name)
    table = await vector_db.create_table(table_name, schema=schema)
    return table


# --8<-- [end:create-table]
# --8<-- [start:embedding-schema]
def make_record_schema(model: str) -> type[LanceModel]:
    model = get_registry().get("huggingface").create(name=model)

    class RecordSchema(LanceModel):
        doc_id: str
        content: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    return RecordSchema


# --8<-- [end:embedding-schema]
async def create_vectorization_tasks(
    project: str,
    *,
    model: str = "BAAI/bge-small-en-v1.5",
    es_client: ESClient | None = None,
    task_client: DatashareTaskClient | None = None,
    vector_db: LanceDBConnection | None = None,
    batch_size: int = 16,
) -> list[str]:
    if es_client is None:
        es_client = lifespan_es_client()
    if task_client is None:
        task_client = lifespan_task_client()
    if vector_db is None:
        vector_db = lifespan_vector_db()
    schema = make_record_schema(model)
    await recreate_vector_table(vector_db, schema)
    # --8<-- [start:query-docs]
    query = make_document_query(match_all())
    # --8<-- [end:query-docs]
    # --8<-- [start:retrieve-docs]
    docs_pages = es_client.poll_search_pages(
        index=project, body=query, sort="_doc:asc", _source=False
    )
    doc_ids = (doc[ID_] async for doc in _flatten_search_pages(docs_pages))
    batches = async_batches(doc_ids, batch_size=batch_size)
    # --8<-- [end:retrieve-docs]
    logging.info("spawning vectorization tasks...")
    # --8<-- [start:batch-vectorization]
    args = {"project": project}
    task_ids = []
    async for batch in batches:
        args["docs"] = list(batch)
        task_id = await task_client.create_task(
            "vectorize_docs", args, group=PYTHON_TASK_GROUP.name
        )
        task_ids.append(task_id)
    logging.info("created %s vectorization tasks !", len(task_ids))
    return task_ids
    # --8<-- [end:batch-vectorization]


async def _flatten_search_pages(pages: AsyncIterable[dict]) -> AsyncIterator[dict]:
    async for page in pages:
        for doc in page[HITS][HITS]:
            yield doc


# --8<-- [end:create_vectorization_tasks]
# --8<-- [start:vectorize_docs]
async def vectorize_docs(
    docs: list[str],
    project: str,
    *,
    es_client: ESClient | None = None,
    vector_db: LanceDBConnection | None = None,
) -> int:
    if es_client is None:
        es_client = lifespan_es_client()
    if vector_db is None:
        vector_db = lifespan_vector_db()
    n_docs = len(docs)
    logging.info("vectorizing %s docs...", n_docs)
    # --8<-- [start:retrieve-doc-content]
    query = {QUERY: ids_query(docs)}
    docs_pages = es_client.poll_search_pages(
        index=project, body=query, sort="_doc:asc", _source_includes=[DOC_CONTENT]
    )
    es_docs = _flatten_search_pages(docs_pages)
    # --8<-- [end:retrieve-doc-content]
    # --8<-- [start:vectorization]
    table = await vector_db.open_table("ds_docs")
    records = [
        {"doc_id": d[ID_], "content": d[SOURCE][DOC_CONTENT]} async for d in es_docs
    ]
    await table.add(records)
    # --8<-- [end:vectorization]
    logging.info("vectorized %s docs !", n_docs)
    return n_docs


# --8<-- [end:vectorize_docs]
# --8<-- [start:find_most_similar]
async def find_most_similar(
    queries: list[str],
    model: str,
    *,
    vector_db: LanceDBConnection | None = None,
    n_similar: int = 2,
) -> list[list[dict]]:
    if vector_db is None:
        vector_db = lifespan_vector_db()
    n_queries = len(queries)
    logging.info("performing similarity search for %s queries...", n_queries)
    table = await vector_db.open_table("ds_docs")
    # Create indexes for hybrid search
    try:
        await table.create_index(
            "vector", config=IvfPq(distance_type="cosine"), replace=False
        )
        await table.create_index("content", config=FTS(), replace=False)
    except RuntimeError:
        logging.debug("skipping index creation as they already exist")
    vectorizer = SentenceTransformer(model)
    vectors = vectorizer.encode(queries)
    futures = (
        _find_most_similar(table, q, v, n_similar) for q, v in zip(queries, vectors)
    )
    results = await asyncio.gather(*futures)
    results = sum(results, start=[])
    logging.info("completed similarity search for %s queries !", n_queries)
    return results


# --8<-- [start:hybrid-search]
async def _find_most_similar(
    table: AsyncTable, query: str, vector: np.ndarray, n_similar: int
) -> list[dict]:
    # pylint: disable=unused-argument
    most_similar = (
        await table.query()
        # The async client seems to be bugged and does really support hybrid queries
        # .nearest_to_text(query, columns=["content"])
        .nearest_to(vector)
        .limit(n_similar)
        .select(["doc_id"])
        .to_list()
    )
    most_similar = [
        {"doc_id": s["doc_id"], "distance": s["_distance"]} for s in most_similar
    ]
    return most_similar


# --8<-- [end:hybrid-search]
# --8<-- [end:find_most_similar]
