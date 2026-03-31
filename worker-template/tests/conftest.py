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
from datashare_python.worker import datashare_worker
from icij_common.es import ESClient
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker
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
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    worker_id = f"test-io-worker-{uuid.uuid4()}"
    pong_activity = Pong(temporal_client=temporal_client, event_loop=event_loop)
    activities = CreateTranslationBatches(
        es_client=es_client,
        temporal_client=temporal_client,
        event_loop=event_loop,
    )
    io_activities = [
        pong_activity.pong,
        activities.create_translation_batches,
        CreateClassificationBatches(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).create_classification_batches,
    ]
    workflows = [PingWorkflow, TranslateAndClassifyWorkflow]
    worker = datashare_worker(
        temporal_client,
        task_queue=TaskQueues.IO,
        workflows=workflows,
        activities=io_activities,
        worker_id=worker_id,
    )
    async with worker:
        yield worker


@pytest.fixture(scope="session")
async def translation_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
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
    worker = datashare_worker(
        temporal_client,
        worker_id=worker_id,
        task_queue=TaskQueues.TRANSLATE_GPU,
        activities=translation_activities,
    )
    async with worker:
        yield worker


@pytest.fixture(scope="session")
async def classification_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
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
    worker = datashare_worker(
        temporal_client,
        worker_id=worker_id,
        task_queue=TaskQueues.CLASSIFY_GPU,
        activities=classification_activities,
    )
    async with worker:
        yield worker
