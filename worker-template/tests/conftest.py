import asyncio
import uuid
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
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
from icij_common.es import ESClient
from temporalio.client import Client as TemporalClient
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker
from worker_template.classify import ClassifyDocs, CreateClassificationBatches
from worker_template.config_ import TranslateAndClassifyWorkerConfig
from worker_template.translate import CreateTranslationBatches, TranslateDocs
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
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    worker_id = f"test-io-worker-{uuid.uuid4()}"
    pong_activity = Pong(temporal_client=temporal_client, event_loop=event_loop)
    io_activities = [
        pong_activity.pong,
        CreateTranslationBatches(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).create_translation_batches,
        CreateClassificationBatches(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).create_classification_batches,
    ]
    workflows = [PingWorkflow, TranslateAndClassifyWorkflow]
    worker = Worker(
        temporal_client,
        identity=worker_id,
        task_queue=TaskQueues.CPU,
        activities=io_activities,
        workflows=workflows,
    )
    async with worker:
        t = None
        try:
            t = asyncio.create_task(worker.run())
            yield
        except Exception:  # noqa: BLE001
            if t is not None:
                t.cancel()


@pytest.fixture(scope="session")
async def translation_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    worker_id = f"test-translation-worker-{uuid.uuid4()}"
    translation_activities = [
        TranslateDocs(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).translate_docs,
    ]
    with ThreadPoolExecutor() as executor:
        worker = Worker(
            temporal_client,
            identity=worker_id,
            task_queue=TaskQueues.TRANSLATE_GPU,
            activities=translation_activities,
            activity_executor=executor,
        )
        async with worker:
            t = None
            try:
                t = asyncio.create_task(worker.run())
                yield
            except Exception:  # noqa: BLE001
                if t is not None:
                    t.cancel()


@pytest.fixture(scope="session")
async def classification_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    worker_id = f"test-classification-worker-{uuid.uuid4()}"
    classification_activities = [
        ClassifyDocs(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).classify_docs,
    ]
    with ThreadPoolExecutor() as executor:
        worker = Worker(
            temporal_client,
            identity=worker_id,
            task_queue=TaskQueues.CLASSIFY_GPU,
            activities=classification_activities,
            activity_executor=executor,
        )
        async with worker:
            t = None
            try:
                t = asyncio.create_task(worker.run())
                yield
            except Exception:  # noqa: BLE001
                if t is not None:
                    t.cancel()


@pytest.fixture(scope="session")
def activity_environment() -> ActivityEnvironment:
    return ActivityEnvironment()
