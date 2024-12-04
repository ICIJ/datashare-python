import logging
from typing import List

from icij_common.es import ESClient, HITS

from ml_worker.objects import Document
from ml_worker.tasks import translate_docs
from ml_worker.tests.conftest import TEST_PROJECT

logger = logging.getLogger(__name__)


async def _progress(p: float):
    logger.info("progress: %s", p)


async def test_translate_docs_integration(
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_es_client: ESClient,
):
    # Given
    es_client = test_es_client
    # When
    n_docs = await translate_docs(
        "ENGLISH", es_client, project=TEST_PROJECT, progress=_progress
    )
    # Then
    body = {"query": {"exists": {"field": "content_translated"}}}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]

    # Then
    assert n_docs == 2
    assert len(index_docs) == 2
    index_docs = [Document.from_es(doc) for doc in index_docs]
    assert all(doc.content_translated for doc in index_docs)
    doc_ids = [doc.id for doc in index_docs]
    assert doc_ids == ["doc-2", "doc-3"]
