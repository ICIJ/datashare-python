import uuid
from collections.abc import AsyncGenerator

import datashare_python
import pytest
from datashare_python.config import (
    DatashareClientConfig,
    LogFormat,
    LoggingConfig,
    TemporalClientConfig,
    WorkerConfig,
)
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    dev_worker_context,
    doc_0,
    doc_1,
    doc_2,
    doc_3,
    indexed_docs,
    populate_es,
    pytest_collection_modifyitems,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client,
    test_temporal_client_session,
    text_0,
    text_1,
)
from temporalio.client import Client as TemporalClient
from worker_template.config_ import TranslateAndClassifyWorkerConfig
from worker_template.workflows import (
    TaskQueues,
)


@pytest.fixture(scope="session")
def test_worker_config() -> TranslateAndClassifyWorkerConfig:
    logging_config = LoggingConfig(
        loggers={datashare_python.__name__: "INFO", __name__: "INFO"},
        format=LogFormat.DEFAULT,
    )
    return TranslateAndClassifyWorkerConfig(
        logging=logging_config,
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
    )


@pytest.fixture(scope="session")
async def workflows_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-workflows-worker-{uuid.uuid4()}"
    task_queue = TaskQueues.WORKFLOWS
    workflows = ["translate-and-classify", "ping"]
    dependencies = "base"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        workflows=workflows,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    dependencies = "base"
    worker_id = "worker-template-io"
    io_activities = [
        "pong-async",
        "create-translation-batches",
        "create-classification-batches",
    ]
    task_queue = TaskQueues.IO
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=io_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def cpu_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    dependencies = "base"
    worker_id = "worker-template-cpu"
    cpu_activities = ["pong-sync"]
    task_queue = TaskQueues.CPU
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=False,
        activities=cpu_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def translation_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = "worker-template-translation"
    translation_activities = ["translate-docs"]
    task_queue = TaskQueues.TRANSLATE_GPU
    deps = "base"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=translation_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=deps,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def classification_worker(
    test_worker_config: WorkerConfig,
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = "worker-template-classification"
    classification_activities = ["classify-docs"]
    task_queue = TaskQueues.CLASSIFY_GPU
    deps = "base"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=classification_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=deps,
    )
    async with worker_ctx:
        yield
