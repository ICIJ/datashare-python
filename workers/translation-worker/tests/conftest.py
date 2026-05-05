import asyncio
import uuid
from collections.abc import AsyncGenerator

import datashare_python
import pytest
import translation_worker
from _pytest.tmpdir import TempPathFactory
from datashare_python.config import (
    DatashareClientConfig,
    LogFormat,
    LoggingConfig,
    TemporalClientConfig,
)
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    event_loop,
    index_docs,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from datashare_python.objects import Document
from datashare_python.types_ import ContextManagerFactory, TemporalClient
from datashare_python.worker import worker_context
from icij_common.es import ESClient
from translation_worker.activities import TranslationActivities
from translation_worker.dependencies import set_es_client, set_worker_config
from translation_worker.objects import TranslationWorkerConfig
from translation_worker.workflows import TaskQueue, TranslationWorkflow


@pytest.fixture(scope="session")
def test_deps() -> list[ContextManagerFactory]:
    return [set_worker_config, set_es_client]


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> TranslationWorkerConfig:  # noqa: ANN001, ARG001, F811
    tmp_path = tmp_path_factory.mktemp("test-")
    audios_root = tmp_path / "audios"
    audios_root.mkdir()
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    logging_config = LoggingConfig(
        loggers={
            datashare_python.__name__: "DEBUG",
            translation_worker.__name__: "DEBUG",
        },
        format=LogFormat.DEFAULT,
    )
    return TranslationWorkerConfig(
        logging=logging_config,
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
        artifacts_root=artifacts_root,
        workdir=workdir,
    )


EN = "en"
FR = "fr"
ES = "es"
ENGLISH = "english"
FRENCH = "french"
SPANISH = "spanish"
DOC_ID_1 = "doc_id_1"
DOC_ID_2 = "doc_id_2"
ROOT_DOCUMENT_1 = "root_document_1"
ROOT_DOCUMENT_2 = "root_document_2"

MOCK_TRANSLATIONS = [
    (DOC_ID_1, ROOT_DOCUMENT_1, "1"),
    (DOC_ID_2, ROOT_DOCUMENT_2, "2"),
]

FRENCH_TEXT = (
    "Dans le port d'Amsterdam, il y a des marins qui chantent les rêves "
    "y a des qui les hantent au large d'Amsterdam. Dans le port "
    "d'Amsterdam, il marins qui dorment comme des oriflammes le long "
    "des berges mornes."
)
SPANISH_TEXT = (
    "Besame.... Besame mucho.... Como si fuera esta noche la última "
    "vez. Besame.... Besame mucho.... Que tengo miedo perderte, "
    "perderte otra vez."
)


def _create_doc(
    doc_id: str, root_doc: str, text: str, language: str = "ENGLISH"
) -> Document:
    return Document(id=doc_id, root_document=root_doc, language=language, content=text)


@pytest.fixture
async def index_translation_documents(
    test_es_client: ESClient,  # noqa: F811
) -> list[Document]:
    docs = []
    languages = ["FRENCH", "SPANISH"]
    for idx, text in enumerate([FRENCH_TEXT, SPANISH_TEXT]):
        doc_id = f"doc_id_{idx}"
        root_document = f"root_document_{idx}"
        docs.append(_create_doc(doc_id, root_document, text, languages[idx]))
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs


@pytest.fixture(scope="session")
def translation_worker_config() -> TranslationWorkerConfig:
    return TranslationWorkerConfig()


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: TranslationWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-io-worker-{uuid.uuid4()}"
    translation_activities = TranslationActivities(
        temporal_client=client, event_loop=event_loop
    )
    batching_activities = [
        translation_activities.translation_worker_config,
        translation_activities.create_translation_batches,
    ]
    workflows = [TranslationWorkflow]
    task_queue = TaskQueue.IO
    worker_ctx = worker_context(
        worker_id,
        activities=batching_activities,
        workflows=workflows,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        dependencies=test_deps,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def translation_inference_worker(
    test_worker_config: TranslationWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-cpu-worker-{uuid.uuid4()}"
    create_translation_batches = TranslationActivities(
        temporal_client=client, event_loop=event_loop
    )
    translation_activities = [create_translation_batches.translate_docs]
    task_queue = TaskQueue.INFERENCE
    worker_ctx = worker_context(
        worker_id,
        activities=translation_activities,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        dependencies=test_deps,
    )
    async with worker_ctx:
        yield
