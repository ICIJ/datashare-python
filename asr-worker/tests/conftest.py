import os
from pathlib import Path

import pytest
from asr_worker.config import ASRWorkerConfig
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    event_loop,
    index_docs,
    populate_es,
    test_deps,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from icij_common.es import ESClient
from icij_common.test_utils import reset_env  # noqa: F401
from objects import Document


@pytest.fixture
def mocked_worker_config_in_env(reset_env, tmp_path: Path) -> ASRWorkerConfig:  # noqa: ANN001, ARG001, F811
    os.environ["DS_WORKER_AUDIO_ROOT"] = str(tmp_path / "audios")
    os.environ["DS_WORKER_ARTIFACT_ROOT"] = str(tmp_path / "artifacts")
    os.environ["DS_WORKER_WORKDIR"] = str(tmp_path / "workdir")
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
