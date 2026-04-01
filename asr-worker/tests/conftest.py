import os
from pathlib import Path

import pytest
from asr_worker.config import ASRWorkerConfig
from asr_worker.dependencies import set_multiprocessing_start_method
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
from icij_common.test_utils import reset_env  # noqa: F401


@pytest.fixture(scope="session")
def test_deps() -> list[ContextManagerFactory]:
    return [set_temporal_client, set_es_client, set_multiprocessing_start_method]


@pytest.fixture
def mocked_worker_config_in_env(reset_env, tmp_path: Path) -> ASRWorkerConfig:  # noqa: ANN001, ARG001, F811
    audios_path = tmp_path / "audios"
    audios_path.mkdir()
    os.environ["DS_WORKER_AUDIOS_ROOT"] = str(audios_path)
    artifacts_path = tmp_path / "artifacts"
    artifacts_path.mkdir()
    os.environ["DS_WORKER_ARTIFACTS_ROOT"] = str(artifacts_path)
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    os.environ["DS_WORKER_WORKDIR"] = str(workdir)
    return ASRWorkerConfig()


@pytest.fixture(scope="session")
def doc_0() -> Document:
    return Document(
        id="doc-0", root_document="root-0", language="ENGLISH", content_type="audio/wav"
    )


@pytest.fixture(scope="session")
def doc_1() -> Document:
    return Document(
        id="doc-1",
        root_document="root-1",
        language="ENGLISH",
        content_type="application/json",
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2", root_document="root-2", language="FRENCH", content_type="audio/mpeg"
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
