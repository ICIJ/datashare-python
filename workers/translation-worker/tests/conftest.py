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
    dev_worker_context,
    index_docs,
    pytest_collection_modifyitems,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client,
    test_temporal_client_session,
    test_worker_config,
)
from datashare_python.objects import DatashareLanguage, Document, WorkerPaths
from datashare_python.types_ import TemporalClient
from icij_common.es import ESClient
from translation_worker.activities import Activity
from translation_worker.config import TranslationWorkerConfig
from translation_worker.constants import TRANSLATION_WORKFLOW_NAME
from translation_worker.workflows import TaskQueue


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> TranslationWorkerConfig:  # noqa: ANN001, ARG001, F811
    tmp_path = tmp_path_factory.mktemp("test-")
    filesystem = tmp_path / "audios"
    filesystem.mkdir()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    worker_paths = WorkerPaths(
        filesystem=filesystem, workdir=workdir, artifacts=artifacts
    )
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
        paths=worker_paths,
    )


ENGLISH = "ENGLISH"
FRENCH = "FRENCH"
SPANISH = "SPANISH"
CHINESE = "CHINESE"
DOC_ID_1 = "doc_id_1"
DOC_ID_2 = "doc_id_2"
ROOT_DOCUMENT_1 = "root_document_1"
ROOT_DOCUMENT_2 = "root_document_2"
DS_ENGLISH = DatashareLanguage(ENGLISH)
DS_FRENCH = DatashareLanguage(FRENCH)
DS_SPANISH = DatashareLanguage(SPANISH)
DS_CHINESE = DatashareLanguage(CHINESE)


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
CHINESE_TEXT = (
    "北京是中国的首都。这是一个美丽的城市，有着悠久的历史和文化。"
    "每年都有数以百万计的游客来这里参观故宫和长城。"
)


def _create_doc(
    doc_id: str, root_doc: str, text: str, language: DatashareLanguage = DS_ENGLISH
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


@pytest.fixture
async def index_chinese_translation_documents(
    test_es_client: ESClient,  # noqa: F811
) -> list[Document]:
    docs = [_create_doc(DOC_ID_1, ROOT_DOCUMENT_1, CHINESE_TEXT, DS_CHINESE)]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs


@pytest.fixture(scope="session")
def translation_worker_config() -> TranslationWorkerConfig:
    return TranslationWorkerConfig()


@pytest.fixture(scope="session")
async def workflows_worker(
    test_worker_config: TranslationWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-io-worker-{uuid.uuid4()}"
    workflows = [TRANSLATION_WORKFLOW_NAME]
    task_queue = TaskQueue.WORKFLOWS
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        workflows=workflows,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: TranslationWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-io-worker-{uuid.uuid4()}"
    batching_activities = [Activity.WORKER_CONFIG, Activity.CREATE_TRANSLATION_BATCHES]
    task_queue = TaskQueue.IO
    dependencies = "translation.io"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=batching_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def translation_inference_worker(
    test_worker_config: TranslationWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-translation-cpu-worker-{uuid.uuid4()}"
    translation_activities = [Activity.TRANSLATE_DOCS]
    task_queue = TaskQueue.C2TRANSLATE_INFERENCE
    dependencies = "translation.inference"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=translation_activities,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield
