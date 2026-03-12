import asyncio
import uuid
from collections.abc import AsyncGenerator

import pytest
from asr_worker.activities import ASRActivities
from asr_worker.objects import TaskQueues
from asr_worker.workflows import ASRWorkflow
from datashare_python.conftest import (  # noqa: F401
    event_loop,
    index_docs,
    test_deps,
    test_es_client,
    test_es_client_session,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from datashare_python.types_ import TemporalClient
from temporalio.worker import Worker


@pytest.fixture(scope="session")
async def cpu_worker(
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    temporal_client = test_temporal_client_session
    cpu_worker_id = f"test-asr-worker-{uuid.uuid4()}"
    asr_activities = ASRActivities(
        temporal_client=temporal_client,
        event_loop=event_loop,
    )
    asr_activities_list = [
        asr_activities.preprocess,
        asr_activities.postprocess,
    ]
    workflows = [ASRWorkflow]
    cpu_worker = Worker(
        temporal_client,
        identity=cpu_worker_id,
        task_queue=TaskQueues.CPU,
        activities=asr_activities_list,
        workflows=workflows,
    )
    async with cpu_worker:
        t = None
        try:
            t = asyncio.create_task(cpu_worker.run())
            yield
        except Exception as e:  # noqa: BLE001
            if t is not None:
                t.cancel()
            raise e


@pytest.fixture(scope="session")
async def gpu_worker(
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    temporal_client = test_temporal_client_session
    gpu_worker_id = f"test-asr-worker-{uuid.uuid4()}"
    asr_activities = ASRActivities(
        temporal_client=temporal_client,
        event_loop=event_loop,
    )
    asr_activities_list = [
        asr_activities.infer,
    ]
    workflows = [ASRWorkflow]
    gpu_worker = Worker(
        temporal_client,
        identity=gpu_worker_id,
        task_queue=TaskQueues.GPU,
        activities=asr_activities_list,
        workflows=workflows,
    )
    async with gpu_worker:
        t = None
        try:
            t = asyncio.create_task(gpu_worker.run())
            yield
        except Exception as e:  # noqa: BLE001
            if t is not None:
                t.cancel()
            raise e
