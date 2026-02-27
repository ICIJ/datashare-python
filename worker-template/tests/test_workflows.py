import uuid
from functools import partial

import pytest
from datashare_python.conftest import TEST_PROJECT, has_state
from datashare_python.objects import Document, TaskState
from datashare_python.task_client import DatashareTaskClient
from icij_common.es import HITS, ESClient, has_type
from icij_common.test_utils import async_true_after
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker
from worker_template.objects_ import (
    TranslateAndClassifyRequest,
    TranslateAndClassifyResponse,
)
from worker_template.workflows import (
    PingWorkflow,
    TaskQueues,
    TranslateAndClassifyWorkflow,
)


@pytest.mark.e2e
async def test_ping_workflow_e2e(
    io_worker: Worker,  # noqa: ARG001
    test_temporal_client_session: TemporalClient,
) -> None:
    # Given
    temporal_client = test_temporal_client_session
    wf_id = f"ping-{uuid.uuid4()}"

    # When
    response = await temporal_client.execute_workflow(
        PingWorkflow, id=wf_id, task_queue=TaskQueues.CPU, request_eager_start=True
    )

    # Then
    assert response == "pong"


@pytest.mark.xfail(reason="expected to fail until DS server is fixed")
@pytest.mark.e2e
async def test_ping_e2e(
    io_worker: Worker,  # noqa: ARG001
    test_task_client: DatashareTaskClient,
) -> None:
    # Given
    task_group = TaskQueues.CPU
    # When
    ping_task_id = await test_task_client.create_task("ping", dict(), group=task_group)
    # Then
    ping_timeout_s = 5.0
    assert await async_true_after(
        partial(
            has_state,
            test_task_client,
            ping_task_id,
            TaskState.DONE,
            fail_early=TaskState.ERROR,
        ),
        after_s=ping_timeout_s,
    )
    # When
    ping_result = await test_task_client.get_task_result(ping_task_id)
    # Then
    assert ping_result == "pong"


@pytest.mark.e2e
async def test_translate_and_classify_workflow_e2e(
    test_temporal_client_session: TemporalClient,
    populate_es: list[Document],  # noqa: ARG001
    test_es_client: ESClient,
    io_worker: Worker,  # noqa: ARG001
    translation_worker: Worker,  # noqa: ARG001
    classification_worker: Worker,  # noqa: ARG001
) -> None:
    # Given
    temporal_client = test_temporal_client_session
    payload = TranslateAndClassifyRequest(project=TEST_PROJECT, language="ENGLISH")
    wf_id = f"translate-and-classify-{uuid.uuid4()}"

    # When
    response = await temporal_client.execute_workflow(
        TranslateAndClassifyWorkflow.run,
        payload,
        id=wf_id,
        task_queue=TaskQueues.CPU,
    )

    # Then
    expected_response = TranslateAndClassifyResponse(translated=2, classified=4)
    assert response == expected_response

    body = {"query": has_type(type_field="type", type_value="Document")}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]

    assert len(index_docs) == 4
    index_docs = [Document.from_es(doc) for doc in index_docs]
    assert all(doc.tags for doc in index_docs)
