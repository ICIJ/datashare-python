import asyncio
import uuid
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from datashare_python.config import (
    DatashareClientConfig,
    TemporalClientConfig,
    WorkerConfig,
)
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    doc_0,
    doc_1,
    doc_2,
    doc_3,
    event_loop,
    populate_es,
    test_deps,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client_session,
    text_0,
    text_1,
    worker_lifetime_deps,
)
from datashare_python.dependencies import (
    with_dependencies,
)
from datashare_python.types_ import ContextManagerFactory
from datashare_python.worker import bootstrap_worker
from temporalio.client import Client as TemporalClient
from worker_template.activities import (
    ClassifyDocs,
    CreateClassificationBatches,
    CreateTranslationBatches,
    TranslateDocs,
)
from worker_template.config_ import TranslateAndClassifyWorkerConfig
from worker_template.workflows import (
    PingWorkflow,
    Pong,
    TaskQueues,
    TranslateAndClassifyWorkflow,
)


@pytest.fixture(scope="session")
def test_worker_config() -> TranslateAndClassifyWorkerConfig:
    return TranslateAndClassifyWorkerConfig(
        log_level="DEBUG",
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
    )


@pytest.fixture(scope="session")
async def lifetime_deps(
    event_loop: AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
    test_worker_config: WorkerConfig,
) -> AsyncGenerator[None, Any]:
    ctx = "unit test application"
    worker_id = f"test-worker-{uuid.uuid4()}"
    async with with_dependencies(
        test_deps,
        worker_config=test_worker_config,
        event_loop=event_loop,
        worker_id=worker_id,
        ctx=ctx,
    ):
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-io-worker-{uuid.uuid4()}"
    pong_activity = Pong(temporal_client=client, event_loop=event_loop)
    io_activities = [
        pong_activity.pong,
        CreateTranslationBatches.create_translation_batches,
        CreateClassificationBatches.create_classification_batches,
    ]
    workflows = [PingWorkflow, TranslateAndClassifyWorkflow]
    task_queue = TaskQueues.IO
    async with (
        bootstrap_worker(
            worker_id,
            activities=io_activities,
            workflows=workflows,
            bootstrap_config=test_worker_config,
            client=client,
            event_loop=event_loop,
            task_queue=task_queue,
            dependencies=test_deps,
        ) as worker,
        worker,
    ):
        yield


@pytest.fixture(scope="session")
async def translation_worker(
    test_worker_config: WorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-worker-{uuid.uuid4()}"
    translation_activities = [TranslateDocs.translate_docs]
    task_queue = TaskQueues.TRANSLATE_GPU
    async with (
        bootstrap_worker(
            worker_id,
            activities=translation_activities,
            bootstrap_config=test_worker_config,
            client=client,
            event_loop=event_loop,
            task_queue=task_queue,
            dependencies=test_deps,
        ) as worker,
        worker,
    ):
        yield


@pytest.fixture(scope="session")
async def classification_worker(
    test_worker_config: WorkerConfig,
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-classification-worker-{uuid.uuid4()}"
    classification_activities = [ClassifyDocs.classify_docs]
    task_queue = TaskQueues.CLASSIFY_GPU
    async with (
        bootstrap_worker(
            worker_id,
            activities=classification_activities,
            bootstrap_config=test_worker_config,
            client=client,
            event_loop=event_loop,
            task_queue=task_queue,
            dependencies=test_deps,
        ) as worker,
        worker,
    ):
        yield
