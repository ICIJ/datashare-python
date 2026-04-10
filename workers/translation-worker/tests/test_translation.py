import traceback

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document
from icij_common.es import HITS, ESClient, has_type
from temporalio.client import Client as TemporalClient
from temporalio.client import WorkflowFailureError
from temporalio.worker import Worker
from translation_worker.objects import TaskQueues, TranslationArgs
from translation_worker.workflows import TranslationWorkflow


@pytest.mark.e2e
async def test_translate_docs(
    test_temporal_client_session: TemporalClient,  # noqa: ARG001
    index_translation_documents: list[Document],  # noqa: ARG001
    test_es_client: ESClient,
    batching_worker: Worker,  # noqa: ARG001
    translation_worker: Worker,  # noqa: ARG001
) -> None:
    """Test translation"""
    req = TranslationArgs(
        project=TEST_PROJECT,
        target_language="english",
    )

    # When
    try:
        res = await test_temporal_client_session.execute_workflow(
            TranslationWorkflow.run, req, id="test-001", task_queue=TaskQueues.CPU
        )

        assert res.num_translations == 2

        body = {"query": has_type(type_field="type", type_value="Document")}
        sort = "_doc:asc"
        index_docs = []
        async for hits in test_es_client.poll_search_pages(
            index=TEST_PROJECT, body=body, sort=sort
        ):
            index_docs += hits[HITS][HITS]
        assert len(index_docs) == 2
        index_docs = [Document.from_es(doc) for doc in index_docs]
        assert all("en" in doc.content_translated for doc in index_docs)
    except WorkflowFailureError:
        pytest.fail(traceback.format_exc())
