import asyncio
import mimetypes
import shutil
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import datashare_python
import pytest
from _pytest.tmpdir import TempPathFactory
from datashare_python.config import (
    DatashareClientConfig,
    LogFormat,
    LoggingConfig,
    TemporalClientConfig,
)
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    clear_dirs,
    doc_3,
    event_loop,
    index_docs,
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
from datashare_python.objects import Document
from datashare_python.types_ import ContextManagerFactory, TemporalClient
from datashare_python.utils import artifacts_dir
from datashare_python.worker import worker_context
from extract_core.objects import SupportedExt
from extract_worker.activities import MarkdownExtract
from extract_worker.config import ExtractWorkerConfig
from extract_worker.constants import TaskQueue
from extract_worker.dependencies import DEPENDENCIES
from extract_worker.objects import ProcessedDoc
from extract_worker.workflows import ExtractMarkdownContentWorkflow
from icij_common.es import ESClient

from tests import DOCS_PATH

mimetypes.init()


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> ExtractWorkerConfig:  # noqa: ANN001, ARG001, F811
    tmp_path = Path(tmp_path_factory.mktemp("test-"))
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    logging_config = LoggingConfig(
        loggers={datashare_python.__name__: "INFO", __name__: "DEBUG"},
        format=LogFormat.DEFAULT,
    )
    return ExtractWorkerConfig(
        logging=logging_config,
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
        docs_root=docs_root,
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
        content_type="application/pdf",
        path=Path("doc-0.pdf"),
        metadata={
            "tika_metadata_resourcename": "doc-0.pdf",
            "tika_metadata_xmptpg_npages": 2,
        },
    )


@pytest.fixture(scope="session")
def doc_1() -> Document:
    return Document(
        id="doc-1",
        root_document="root-1",
        index=TEST_PROJECT,
        language="ENGLISH",
        content_type="audio/wav",
        path=Path("doc-1.wav"),
        metadata={"tika_metadata_resourcename": "doc-1.wav"},
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2",
        index=TEST_PROJECT,
        language="FRENCH",
        content_type=mimetypes.guess_type("doc-2.docx")[0],
        path=Path("doc-2.docx"),
        metadata={"tika_metadata_resourcename": "doc-2.docx"},
    )


@pytest.fixture
async def populate_es(
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


@pytest.fixture
def docs_with_cached_artifacts(
    populate_es: list[Document],
    test_worker_config: ExtractWorkerConfig,
) -> list[ProcessedDoc]:
    config = test_worker_config
    clear_dirs(test_worker_config)
    supported_exts = {SupportedExt.PDF, SupportedExt.DOCX}
    docs = [
        d for d in populate_es if d.path is not None and d.path.suffix in supported_exts
    ]
    paths = []
    for doc in docs:
        doc_path = DOCS_PATH / doc.path
        if doc.root_document is None:
            config.docs_root.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, config.docs_root / doc.path)
        else:
            artifact_path = (
                config.artifacts_root / artifacts_dir(doc.id, project=doc.index) / "raw"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, artifact_path)
        fs_doc = doc.to_filesystem()
        paths.append(fs_doc)
    return paths


@pytest.fixture(scope="session")
async def workflows_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
    test_deps: list[ContextManagerFactory],  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-workflows-worker-{uuid.uuid4()}"
    task_queue = TaskQueue.WORKFLOWS
    worker_ctx = worker_context(
        worker_id,
        workflows=[ExtractMarkdownContentWorkflow],
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        dependencies=test_deps,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-io-worker-{uuid.uuid4()}"
    acts = MarkdownExtract(temporal_client=client, event_loop=event_loop)
    acts = [acts.extract_worker_config, acts.create_markdown_extract_batches]
    task_queue = TaskQueue.IO
    worker_ctx = worker_context(
        worker_id,
        activities=acts,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        dependencies=DEPENDENCIES["extract.io"],
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def md_extract_cpu_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-cpu-worker-{uuid.uuid4()}"
    acts = MarkdownExtract(temporal_client=client, event_loop=event_loop)
    acts = [acts.extract_markdown_content]
    task_queue = TaskQueue.EXTRACT_CPU
    worker_ctx = worker_context(
        worker_id,
        activities=acts,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        dependencies=DEPENDENCIES["extract.extract"],
    )
    async with worker_ctx:
        yield
