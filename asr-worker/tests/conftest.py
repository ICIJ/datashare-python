from pathlib import Path

import pytest
from _pytest.tmpdir import TempPathFactory
from asr_worker.config import ASRWorkerConfig
from asr_worker.dependencies import set_multiprocessing_start_method
from datashare_python.config import DatashareClientConfig, TemporalClientConfig
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    doc_3,
    event_loop,
    index_docs,
    populate_es,
    test_es_client,
    test_es_client_session,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from datashare_python.dependencies import set_es_client, set_temporal_client
from datashare_python.objects import Document
from datashare_python.types_ import ContextManagerFactory
from icij_common.es import ESClient


@pytest.fixture(scope="session")
def test_deps() -> list[ContextManagerFactory]:
    return [set_temporal_client, set_es_client, set_multiprocessing_start_method]


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> ASRWorkerConfig:  # noqa: ANN001, ARG001, F811
    tmp_path = tmp_path_factory.mktemp("test-")
    audios_root = tmp_path / "audios"
    audios_root.mkdir()
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    return ASRWorkerConfig(
        log_level="DEBUG",
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
        audios_root=audios_root,
        artifacts_root=artifacts_root,
        workdir=workdir,
    )


@pytest.fixture(scope="session")
def doc_0() -> Document:
    return Document(
        id="doc-0",
        root_document="root-0",
        language="ENGLISH",
        content_type="audio/wav",
        path=Path("doc-0.wav"),
    )


@pytest.fixture(scope="session")
def doc_1() -> Document:
    return Document(
        id="doc-1",
        root_document="root-1",
        language="ENGLISH",
        content_type="application/json",
        path=Path("doc-1.json"),
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2",
        root_document="root-2",
        language="FRENCH",
        content_type="audio/mpeg",
        path=Path("doc-2.mp3"),
    )


@pytest.fixture
async def populate_es_with_audio(
    test_es_client: ESClient,  # noqa: F811
    doc_0: Document,
    doc_1: Document,
    doc_2: Document,
    doc_3: Document,  # noqa: F811
) -> list[Document]:
    docs = [doc_0, doc_1, doc_2, doc_3]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs
