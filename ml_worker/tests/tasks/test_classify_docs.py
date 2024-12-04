import logging
from typing import List

from icij_common.es import ESClient, HITS, has_type

from ml_worker.objects import Document
from ml_worker.tasks import classify_docs
from ml_worker.tests.conftest import TEST_PROJECT

logger = logging.getLogger(__name__)


async def _progress(p: float):
    logger.info("progress: %s", p)


async def test_classify_docs_integration(
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_es_client: ESClient,
):
    # Given
    es_client = test_es_client
    # When
    n_docs = await classify_docs(es_client, project=TEST_PROJECT, progress=_progress)
    # Then
    body = {"query": has_type(type_field="type", type_value="Document")}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]

    # Then
    assert n_docs == 4
    assert len(index_docs) == 4
    index_docs = [Document.from_es(doc) for doc in index_docs]
    assert all(doc.tags for doc in index_docs)
