import logging
import uuid

import datashare_python
from _pytest.logging import LogCaptureFixture
from datashare_python.types_ import TemporalClient
from datashare_python.worker import datashare_worker
from temporalio.worker import PollerBehaviorSimpleMaximum

from .conftest import MockedWorkflow, mocked_act, mocked_async_act


def test_datashare_worker_default(test_temporal_client_session: TemporalClient) -> None:
    # Given
    client = test_temporal_client_session
    task_queue = f"test-{uuid.uuid4()}"
    # When
    worker = datashare_worker(
        client, task_queue=task_queue, activities=[mocked_act], workflows=[]
    )
    # Then
    worker_config = worker.config()
    assert worker_config["task_queue"] == task_queue
    assert worker_config[
        "activity_task_poller_behavior"
    ] == PollerBehaviorSimpleMaximum(1)
    assert worker_config["max_concurrent_activities"] == 1


def test_datashare_worker_should_warn_for_sync_and_async_activities(
    test_temporal_client_session: TemporalClient, caplog: LogCaptureFixture
) -> None:
    # Given
    client = test_temporal_client_session
    task_queue = f"test-{uuid.uuid4()}"
    # When
    with caplog.at_level(logging.WARNING, logger=datashare_python.__name__):
        datashare_worker(
            client, task_queue=task_queue, activities=[mocked_act, mocked_async_act]
        )
    # Then
    expected = (
        "run all CPU-bound activities inside a worker and IO-bound activities "
        "in a separate worker"
    )
    assert any(expected in r.msg for r in caplog.records)


def test_datashare_worker_should_warn_for_sync_activities_and_workflow(
    test_temporal_client_session: TemporalClient, caplog: LogCaptureFixture
) -> None:
    # Given
    client = test_temporal_client_session
    task_queue = f"test-{uuid.uuid4()}"
    # When
    with caplog.at_level(logging.WARNING, logger=datashare_python.__name__):
        datashare_worker(
            client,
            task_queue=task_queue,
            activities=[mocked_act],
            workflows=[MockedWorkflow],
        )
    # Then
    expected = (
        "run all CPU-bound activities inside a worker and workflows + IO-bound "
        "activities in a separate worker"
    )
    assert any(expected in r.msg for r in caplog.records)
