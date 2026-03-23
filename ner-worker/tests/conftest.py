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
    doc_0,
    doc_1,
    doc_2,
    doc_3,
    event_loop,
    populate_es,
    test_deps,
    test_es_client,
    test_es_client_session,
    test_temporal_client_session,
    text_0,
    text_1,
    worker_lifetime_deps,
)
from datashare_python.dependencies import with_dependencies
from datashare_python.types_ import ContextManagerFactory
from icij_common.es import ESClient
from ner_worker.config_ import NERWorkerConfig
from temporalio.client import Client as TemporalClient
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker


@pytest.fixture(scope="session")
def test_worker_config() -> NERWorkerConfig:
    return NERWorkerConfig(
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
async def spacy_worker(
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
def activity_env() -> ActivityEnvironment:
    env = ActivityEnvironment()
    env.payload_converter = PydanticPayloadConverter()
    return env
