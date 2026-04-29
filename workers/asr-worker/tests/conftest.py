import shutil
from pathlib import Path

import asr_worker
import datashare_python
import pytest
from _pytest.tmpdir import TempPathFactory
from asr_worker.config import ASRWorkerConfig
from asr_worker.constants import SUPPORTED_CONTENT_TYPES
from asr_worker.dependencies import set_multiprocessing_start_method
from datashare_python.config import (
    DatashareClientConfig,
    LogFormat,
    LoggingConfig,
    TemporalClientConfig,
)
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
    typer_asyncio_patch,
    worker_lifetime_deps,
)
from datashare_python.dependencies import set_es_client, set_temporal_client
from datashare_python.objects import Document, FilesystemDocument
from datashare_python.types_ import ContextManagerFactory
from datashare_python.utils import artifacts_dir
from icij_common.es import ESClient

from tests import AUDIOS_PATH


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
    logging_config = LoggingConfig(
        loggers={datashare_python.__name__: "INFO", asr_worker.__name__: "INFO"},
        format=LogFormat.DEFAULT,
    )
    return ASRWorkerConfig(
        logging=logging_config,
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
        index=TEST_PROJECT,
        language="ENGLISH",
        content_type="audio/wav",
        path=Path("root-0.eml"),
        metadata={"tika_metadata_resourcename": "doc-0.wav"},
    )


@pytest.fixture(scope="session")
def doc_1() -> Document:
    return Document(
        id="doc-1",
        root_document="root-1",
        index=TEST_PROJECT,
        language="ENGLISH",
        content_type="application/json",
        path=Path("doc-1.json"),
        metadata={"tika_metadata_resourcename": "doc-1.json"},
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2",
        index=TEST_PROJECT,
        language="FRENCH",
        content_type="audio/mpeg",
        path=Path("doc-2.mp3"),
        metadata={"tika_metadata_resourcename": "doc-2.mp3"},
    )


@pytest.fixture
async def populate_es_with_audios(
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


def _clear_dirs(config: ASRWorkerConfig) -> None:
    shutil.rmtree(config.artifacts_root)
    config.artifacts_root.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(config.workdir)
    config.workdir.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def with_audio_docs(
    populate_es_with_audios: list[Document], test_worker_config: ASRWorkerConfig
) -> list[FilesystemDocument]:
    config = test_worker_config
    _clear_dirs(test_worker_config)
    docs = [
        d for d in populate_es_with_audios if d.content_type in SUPPORTED_CONTENT_TYPES
    ]
    paths = []
    audio_path = AUDIOS_PATH / "asr_test.wav"
    for doc in docs:
        if doc.root_document is None:
            config.docs_root.mkdir(parents=True, exist_ok=True)
            shutil.copy(audio_path, config.docs_root / doc.path)
        else:
            artifact_path = (
                config.artifacts_root / artifacts_dir(doc.id, project=doc.index) / "raw"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(audio_path, artifact_path)
        fs_doc = doc.to_filesystem()
        paths.append(fs_doc)
    return paths
