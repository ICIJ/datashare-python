# pylint: disable=redefined-outer-name
import asyncio
import json
import os
from multiprocessing import Process
from pathlib import Path
from typing import AsyncGenerator, Generator, Iterator

import aiohttp
import psutil
import pytest
from aiohttp import ClientTimeout
from elasticsearch._async.helpers import async_streaming_bulk
from icij_common.es import DOC_ROOT_ID, ESClient, ES_DOCUMENT_TYPE, ID
from icij_worker import AMQPWorkerConfig, WorkerBackend
from icij_worker.backend import start_workers
from redis import asyncio as aioredis

from datashare_python.app import PYTHON_TASK_GROUP, app
from datashare_python.config import AppConfig
from datashare_python.objects import Document
from datashare_python.tasks.dependencies import lifespan_es_client

RABBITMQ_TEST_PORT = 5672
RABBITMQ_TEST_HOST = "localhost"
RABBITMQ_DEFAULT_VHOST = "%2F"

_RABBITMQ_MANAGEMENT_PORT = 15672
TEST_MANAGEMENT_URL = f"http://localhost:{_RABBITMQ_MANAGEMENT_PORT}"
_DEFAULT_BROKER_URL = (
    f"amqp://guest:guest@{RABBITMQ_TEST_HOST}:{RABBITMQ_TEST_PORT}/"
    f"{RABBITMQ_DEFAULT_VHOST}"
)
_DEFAULT_AUTH = aiohttp.BasicAuth(login="guest", password="guest", encoding="utf-8")

TEST_PROJECT = "test-project"

_INDEX_BODY = {
    "mappings": {
        "properties": {
            "type": {"type": "keyword"},
            "language": {"type": "keyword"},
            "documentId": {"type": "keyword"},
            "join": {"type": "join", "relations": {"Document": "NamedEntity"}},
        }
    }
}


@pytest.fixture(scope="session")
def event_loop(request) -> Iterator[asyncio.AbstractEventLoop]:
    # pylint: disable=unused-argument
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_app_config() -> AppConfig:
    return AppConfig(log_level="DEBUG", ds_url="http://localhost:8080")


@pytest.fixture(scope="session")
def test_app_config_path(tmpdir_factory, test_app_config: AppConfig) -> Path:
    config_path = Path(tmpdir_factory.mktemp("app_config")).joinpath("app_config.json")
    config_path.write_text(test_app_config.json())
    return config_path


@pytest.fixture(scope="session")
def test_worker_config(test_app_config_path: Path) -> AMQPWorkerConfig:
    return AMQPWorkerConfig(
        log_level="DEBUG", app_bootstrap_config_path=test_app_config_path
    )


@pytest.fixture(scope="session")
async def app_lifetime_deps(event_loop, test_worker_config: AMQPWorkerConfig):
    # pylint: disable=unused-argument
    worker_id = "test-worker-id"
    async with app.lifetime_dependencies(
        worker_config=test_worker_config, worker_id=worker_id
    ):
        yield


@pytest.fixture(scope="session")
async def es_test_client_session(app_lifetime_deps) -> ESClient:
    # pylint: disable=unused-argument
    es = lifespan_es_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


@pytest.fixture()
async def test_es_client(
    es_test_client_session: ESClient,
) -> ESClient:
    es = es_test_client_session
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


@pytest.fixture()
async def test_task_client(test_app_config) -> ESClient:
    task_client = test_app_config.to_task_client()
    async with task_client:
        redis = aioredis.from_url("redis://localhost")
        await redis.flushall()
        await task_client.delete_all_tasks()
        yield task_client


@pytest.fixture(scope="session")
async def rabbit_mq_session() -> AsyncGenerator[str, None]:
    await wipe_rabbit_mq()
    yield _DEFAULT_BROKER_URL


@pytest.fixture()
async def rabbit_mq() -> AsyncGenerator[str, None]:
    await wipe_rabbit_mq()
    yield _DEFAULT_BROKER_URL


@pytest.fixture()
async def populate_es(
    test_es_client: ESClient,
    doc_0: Document,
    doc_1: Document,
    doc_2: Document,
    doc_3: Document,
) -> list[Document]:
    docs = [doc_0, doc_1, doc_2, doc_3]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs


def index_docs_ops(
    docs: list[Document], index_name: str
) -> Generator[dict, None, None]:
    for doc in docs:
        op = {
            "_op_type": "index",
            "_index": index_name,
        }
        doc = doc.dict(by_alias=True)
        op.update(doc)
        op["_id"] = doc[ID]
        op["routing"] = doc[DOC_ROOT_ID]
        op["type"] = ES_DOCUMENT_TYPE
        yield op


async def index_docs(
    client: ESClient, *, docs: list[Document], index_name: str = TEST_PROJECT
) -> AsyncGenerator[dict, None]:
    ops = index_docs_ops(docs, index_name)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


@pytest.fixture(scope="session")
def text_0() -> str:
    return """In this first sentence I'm speaking about a person named Dan.

Then later I'm speaking about Paris and Paris again

To finish I'm speaking about a company named Intel.
"""


@pytest.fixture(scope="session")
def text_1() -> str:
    return "some short document"


@pytest.fixture(scope="session")
def doc_0(text_0: str) -> Document:
    return Document(
        id="doc-0", root_document="root-0", language="ENGLISH", content=text_0
    )


@pytest.fixture(scope="session")
def doc_1(text_1: str) -> Document:
    return Document(
        id="doc-1", root_document="root-1", language="ENGLISH", content=text_1
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2",
        root_document="root-2",
        language="FRENCH",
        content="traduis ce texte en anglais",
    )


@pytest.fixture(scope="session")
def doc_3() -> Document:
    return Document(
        id="doc-3",
        root_document="root-3",
        language="SPANISH",
        content="traduce este texto al inglés",
    )


def _worker_main(config_path: Path, n_workers: int):
    os.environ["_TYPER_STANDARD_TRACEBACK"] = "1"
    start_workers(
        "datashare_python.app.app",
        n_workers,
        config_path,
        backend=WorkerBackend.MULTIPROCESSING,
        group=PYTHON_TASK_GROUP.name,
    )


@pytest.fixture
def worker_pool(
    test_worker_config: AMQPWorkerConfig,
    tmpdir,
    # Clear the tasks
    test_task_client,  # pylint: disable=unused-argument
    # Wipe rabbitMQ
    rabbit_mq,  # pylint: disable=unused-argument
):
    tmp_path = Path(tmpdir) / "config.json"
    config = test_worker_config.model_dump(mode="json")
    config["type"] = test_worker_config.type.default
    tmp_path.write_text(json.dumps(config))
    p = Process(target=_worker_main, args=(tmp_path, 2))
    p.start()
    try:
        yield p
    finally:
        main_process = psutil.Process(p.pid)
        for child in main_process.children(recursive=True):
            child.kill()
        main_process.kill()


def rabbit_mq_test_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        raise_for_status=True, auth=_DEFAULT_AUTH, timeout=ClientTimeout(total=2)
    )


async def wipe_rabbit_mq():
    async with rabbit_mq_test_session() as session:
        tasks = [_empty_all_queues(session)]
        await asyncio.gather(*tasks)


def get_test_management_url(url: str) -> str:
    return f"{TEST_MANAGEMENT_URL}{url}"


async def _empty_all_queues(session: aiohttp.ClientSession):
    url = f"/api/queues/{RABBITMQ_DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        queues = await res.json()
    tasks = [_delete_queue_content(session, q["name"]) for q in queues]
    await asyncio.gather(*tasks)


async def _delete_queue_content(session: aiohttp.ClientSession, name: str):
    url = f"/api/queues/{RABBITMQ_DEFAULT_VHOST}/{name}/contents"
    async with session.delete(get_test_management_url(url)) as res:
        res.raise_for_status()
