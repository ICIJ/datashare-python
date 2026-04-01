import uuid
from datetime import timedelta

import pytest
from datashare_python.types_ import TemporalClient
from datashare_python.utils import activity_defn, positional_args_only
from datashare_python.worker import datashare_worker
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError


@positional_args_only
def hello_world_keyword(*, who: str) -> str:
    return f"hello {who}"


def test_keyword_safe_activity() -> None:
    # When
    try:
        activity.defn(hello_world_keyword)
    except Exception as e:
        raise AssertionError(
            "couldn't create activity from keyword only function "
        ) from e


@activity_defn(name="non_retriable")
async def non_retriable() -> None:
    raise ValueError("non retriable error occurred")


@workflow.defn(name="non_retriable_workflow")
class NonRetriableWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            non_retriable,
            task_queue="io",
            schedule_to_close_timeout=timedelta(seconds=30),
        )


async def test_retriable(test_temporal_client_session: TemporalClient) -> None:
    # Given
    client = test_temporal_client_session
    workflow_id = f"workflow_{uuid.uuid4().hex}"
    worker_id = f"worker-{uuid.uuid4().hex}"
    worker = datashare_worker(
        client,
        worker_id=worker_id,
        task_queue="io",
        workflows=[NonRetriableWorkflow],
        activities=[non_retriable],
    )
    async with worker:
        with pytest.raises(WorkflowFailureError) as ctx:
            await client.execute_workflow(
                NonRetriableWorkflow.run, id=workflow_id, task_queue="io"
            )
        cause = ctx.value.cause.__cause__
        assert isinstance(cause, ApplicationError)
        assert cause.message == "non retriable error occurred"
        assert cause.non_retryable
