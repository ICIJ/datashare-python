import asyncio
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator, Generator, Iterator, Sequence

import aiohttp
import pytest
from elasticsearch._async.helpers import async_streaming_bulk
from icij_common.es import DOC_ROOT_ID, ES_DOCUMENT_TYPE, ID, ESClient

from .config import DatashareClientConfig, TemporalClientConfig, WorkerConfig
from .dependencies import (
    lifespan_es_client,
    lifespan_task_client,
    lifespan_temporal_client,
    with_dependencies,
)
from .objects import Document, TaskState
from .task_client import DatashareTaskClient
from .types_ import ContextManagerFactory, TemporalClient

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


# Override this one in your conftest with your own dependencies
@pytest.fixture(scope="session")
def test_deps() -> list[ContextManagerFactory]:
    return []


@pytest.fixture(scope="session")
def event_loop(
    request: pytest.FixtureRequest,  # noqa: ARG001
) -> Iterator[asyncio.AbstractEventLoop]:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_worker_config() -> WorkerConfig:
    return WorkerConfig(
        log_level="DEBUG",
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
    )


@pytest.fixture(scope="session")
async def worker_lifetime_deps(
    event_loop: AbstractEventLoop,
    test_deps: list[ContextManagerFactory],
    test_worker_config: WorkerConfig,
) -> AsyncGenerator[None, None]:
    worker_id = "test-worker-id"
    ctx = "test application"
    async with with_dependencies(
        test_deps,
        ctx=ctx,
        worker_id=worker_id,
        worker_config=test_worker_config,
        event_loop=event_loop,
    ):
        yield


@pytest.fixture(scope="session")
async def test_es_client_session(
    worker_lifetime_deps,  # noqa: ANN001, ARG001
) -> ESClient:
    es = lifespan_es_client()
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


@pytest.fixture
async def test_es_client(test_es_client_session: ESClient) -> ESClient:
    es = test_es_client_session
    await es.indices.delete(index="_all")
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


@pytest.fixture(scope="session")
async def test_task_client_session(
    worker_lifetime_deps,  # noqa: ANN001, ARG001
) -> AsyncGenerator[DatashareTaskClient, None]:
    task_client = lifespan_task_client()
    async with task_client:
        await task_client.delete_all_tasks()
        yield task_client


@pytest.fixture
async def test_task_client(
    test_task_client_session: DatashareTaskClient,
) -> DatashareTaskClient:
    await test_task_client_session.delete_all_tasks()
    return test_task_client_session


@pytest.fixture(scope="session")
def test_temporal_client_session(
    worker_lifetime_deps,  # noqa: ANN001, ARG001
) -> TemporalClient:
    return lifespan_temporal_client()


@pytest.fixture
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
        doc = doc.model_dump(by_alias=True)  # noqa: PLW2901
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


async def has_state(
    task_client: DatashareTaskClient,
    task_id: str,
    expected_state: TaskState | Sequence[TaskState],
    fail_early: TaskState | Sequence[TaskState] | None = None,
) -> bool:
    if isinstance(expected_state, TaskState):
        expected_state = (expected_state,)
    if isinstance(fail_early, TaskState):
        fail_early = (fail_early,)
    expected_state = set(expected_state)
    if fail_early:
        fail_early = set(fail_early)
    state = await task_client.get_task_state(task_id)
    if fail_early and state in fail_early:
        raise ValueError(f"Found invalid state {state}, expected {expected_state}")
    return state in expected_state


async def all_done(task_client: DatashareTaskClient, not_done: list[str]) -> bool:
    while not_done:
        for t_id in not_done:
            is_done = await has_state(
                task_client, t_id, TaskState.DONE, fail_early=TaskState.ERROR
            )
            if not is_done:
                return False
            not_done.remove(t_id)
    return True
