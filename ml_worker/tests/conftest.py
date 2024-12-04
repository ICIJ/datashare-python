# pylint: disable=redefined-outer-name
import asyncio
from pathlib import Path
from typing import AsyncGenerator, Generator, Iterator

import pytest
from elasticsearch._async.helpers import async_streaming_bulk
from icij_common.es import DOC_ROOT_ID, ESClient, ES_DOCUMENT_TYPE, ID
from icij_worker import AMQPWorkerConfig

from ml_worker.app import app
from ml_worker.config import AppConfig
from ml_worker.objects import Document
from ml_worker.tasks.dependencies import lifespan_es_client

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
    return AppConfig(log_level="DEBUG")


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
        root_document="roo3",
        language="SPANISH",
        content="traduce este texto al inglés",
    )
