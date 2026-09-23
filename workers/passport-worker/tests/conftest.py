import shutil
from collections.abc import AsyncGenerator
from pathlib import Path

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
    dev_worker_context,
    index_docs,
    populate_es,
    pytest_collection_modifyitems,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client,
    test_temporal_client_session,
)
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    DatashareFile,
    DatashareLanguage,
    Document,
    FileLocation,
    WorkerFile,
    WorkerPath,
    WorkerRoots,
)
from datashare_python.types_ import TemporalClient
from datashare_python.utils import artifacts_dir, ext_to_mime_types
from icij_common.es import ESClient
from icij_common.pydantic_utils import safe_copy
from passport_worker.activities import Activity
from passport_worker.config import (
    ImagePreprocessingWorkerConfig,
    PassportWorkerConfig,
    PreprocessingWorkerConfig,
)
from passport_worker.workflows import TaskQueue

from . import DOCS_PATH, TEST_RESOURCE_PATH


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> PassportWorkerConfig:
    tmp_path = tmp_path_factory.mktemp("test-")
    filesystem = tmp_path / "filesystem"
    filesystem.mkdir()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    worker_roots = WorkerRoots(
        filesystem=filesystem, artifacts=artifacts, workdir=workdir
    )
    loggers = {
        "datashare_python": "INFO",
        "passport_service": "INFO",
        "passport_worker": "DEBUG",
        "temporalio": "DEBUG",
    }
    logging_config = LoggingConfig(format=LogFormat.DEFAULT, loggers=loggers)

    return PassportWorkerConfig(
        logging=logging_config,
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
        roots=worker_roots,
        preprocessing=PreprocessingWorkerConfig(
            images=ImagePreprocessingWorkerConfig(n_processes=1)
        ),
    )


DOC_0 = Document(
    index=TEST_PROJECT,
    id="doc-0",
    root_document="root-0",
    extraction_level=1,
    language=DatashareLanguage("ENGLISH"),
    path=Path("not_a_passport.jpg"),
    metadata={TIKA_METADATA_RESOURCENAME: "not_a_passport.jpg"},
    content_type="image/jpeg",
)


DS_FILE_0 = DatashareFile(
    doc_id=DOC_0.doc_id,
    project=DOC_0.project,
    parent=safe_copy(DOC_0, update={"content_type": None}),
    value=WorkerPath(
        path=Path(TEST_PROJECT, "do", "c-", "doc-0", "raw"),
        location=FileLocation.ARTIFACTS,
    ),
    n_pages=1,
)

SYMLINKED_DS_FILE_0 = WorkerFile(
    doc_id=DOC_0.doc_id,
    project=DOC_0.project,
    parent=DS_FILE_0,
    value=WorkerPath(
        path=Path(TEST_PROJECT, "symlinks", "do", "c-", "doc-0", "not_a_passport.jpg"),
        location=FileLocation.WORKDIR,
    ),
    n_pages=1,
)


@pytest.fixture(scope="session")
def doc_0() -> Document:
    return DOC_0


DOC_1 = Document(
    id="doc-1",
    index=TEST_PROJECT,
    language=DatashareLanguage("ENGLISH"),
    path=Path("not_a_passport.pdf"),
    metadata={
        TIKA_METADATA_RESOURCENAME: "not_a_passport.pdf",
        "tika_metadata_xmptpg_npages": 2,
    },
    content_type="application/pdf",
)
# When searching from ES, we drop content type and
DS_FILE_1 = DatashareFile(
    doc_id=DOC_1.doc_id,
    project=DOC_1.project,
    parent=safe_copy(DOC_1, update={"content_type": None}),
    value=WorkerPath(path=Path("not_a_passport.pdf"), location=FileLocation.FILESYSTEM),
    n_pages=2,
)


@pytest.fixture(scope="session")
def doc_1() -> Document:
    return DOC_1


DOC_2 = Document(
    id="doc-2",
    index=TEST_PROJECT,
    language=DatashareLanguage("ENGLISH"),
    path=Path("passport.docx"),
    metadata={TIKA_METADATA_RESOURCENAME: "passport.docx"},
    content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
)
DS_FILE_2 = DatashareFile(
    doc_id=DOC_2.doc_id,
    project=DOC_2.project,
    parent=safe_copy(DOC_2, update={"content_type": None}),
    value=WorkerPath(path=Path("passport.docx"), location=FileLocation.FILESYSTEM),
    n_pages=1,
)


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return DOC_2


@pytest.fixture(scope="session")
def doc_3() -> Document:
    return Document(
        id="doc-3",
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
        path=Path("passport.eml"),
        metadata={TIKA_METADATA_RESOURCENAME: "passport.eml"},
        content_type="message/rfc822",
    )


@pytest.fixture(scope="session")
def doc_4() -> Document:
    return Document(
        id="doc-4",
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
        path=Path("passport.odt"),
        metadata={TIKA_METADATA_RESOURCENAME: "passport.odt"},
        content_type="application/vnd.oasis.opendocument.text",
    )


DOC_5 = Document(
    id="doc-5",
    index=TEST_PROJECT,
    language=DatashareLanguage("ENGLISH"),
    path=Path("passport.pdf"),
    metadata={TIKA_METADATA_RESOURCENAME: "passport.pdf"},
    content_type="application/pdf",
)

DS_FILE_5 = DatashareFile(
    doc_id=DOC_5.doc_id,
    project=DOC_5.project,
    parent=safe_copy(DOC_5, update={"content_type": None}),
    value=WorkerPath(path=Path("passport.pdf"), location=FileLocation.FILESYSTEM),
    n_pages=1,
)


@pytest.fixture(scope="session")
def doc_5() -> Document:
    return DOC_5


@pytest.fixture(scope="session")
def doc_6() -> Document:
    return Document(
        id="doc-6",
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
        path=Path("passport.png"),
        metadata={TIKA_METADATA_RESOURCENAME: "passport.png"},
        content_type="image/png",
    )


@pytest.fixture
async def indexed_docs(  # noqa: PLR0917
    doc_0: Document,
    doc_1: Document,
    doc_2: Document,
    doc_3: Document,
    doc_4: Document,
    doc_5: Document,
    doc_6: Document,
) -> list[Document]:
    return [doc_0, doc_1, doc_2, doc_3, doc_4, doc_5, doc_6]


@pytest.fixture
def docs_with_cached_artifacts(
    populate_es: list[Document],  # noqa: F811
    test_worker_config: PassportWorkerConfig,
) -> list[DatashareFile]:
    config = test_worker_config
    clear_dirs(test_worker_config)
    paths = []
    worker_paths = config.roots
    for doc in populate_es:
        doc_path = DOCS_PATH / doc.path
        if doc.is_root_document:
            worker_paths.filesystem.mkdir(parents=True, exist_ok=True)
            fs_path = worker_paths.filesystem / doc.path
            fs_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, fs_path)
        else:
            artifact_path = (
                worker_paths.artifacts
                / artifacts_dir(doc.id, project=doc.project)
                / "raw"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, artifact_path)
        paths.append(DatashareFile.from_parent(doc))
    return paths


@pytest.fixture(scope="session")
def e2e_doc_from_dir() -> list[Document]:
    docs = []
    for f in DOCS_PATH.iterdir():
        if f.is_file():
            meta = {TIKA_METADATA_RESOURCENAME: f.name}
            content_type = list(ext_to_mime_types(f.suffix))[0]
            doc = Document(
                id=f"e2e-doc-{len(docs)}",
                path=f.relative_to(DOCS_PATH),
                language=DatashareLanguage("ENGLISH"),
                metadata=meta,
                index=TEST_PROJECT,
                content_type=content_type,
            )
            docs.append(doc)
    return docs


@pytest.fixture
async def populate_es_with_e2e_docs(
    test_es_client: ESClient,  # noqa: F811
    e2e_doc_from_dir: list[Document],  # noqa: F811
) -> list[Document]:
    async for _ in index_docs(
        test_es_client, docs=e2e_doc_from_dir, index_name=TEST_PROJECT
    ):
        pass
    return e2e_doc_from_dir


@pytest.fixture
def e2e_docs(
    populate_es_with_e2e_docs: list[Document], test_worker_config: PassportWorkerConfig
) -> list[DatashareFile]:
    config = test_worker_config
    clear_dirs(test_worker_config)
    paths = []
    worker_paths = config.roots
    for doc in populate_es_with_e2e_docs:
        doc_path = DOCS_PATH / doc.path
        if doc.is_root_document:
            worker_paths.filesystem.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, worker_paths.filesystem / doc.path)
        else:
            artifact_path = (
                worker_paths.artifacts
                / artifacts_dir(doc.id, project=doc.project)
                / "raw"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, artifact_path)
        ds_doc = DatashareFile.from_parent(doc)
        paths.append(ds_doc)
    return paths


@pytest.fixture(scope="session")
async def workflows_worker(
    test_worker_config: PassportWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    task_queue = TaskQueue.WORKFLOWS
    worker_ctx = dev_worker_context(
        "test-passport-worklow-worker",
        is_async=True,
        workflows=["passport-detection.detect-passports"],
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: PassportWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    acts = [
        Activity.CREATE_PREPROCESSING_BATCHES,
        Activity.CONVERT_TO_PDFS,
        Activity.PREPROCESS_PDFS,
        Activity.CREATE_INFERENCE_BATCHES,
        Activity.AGGREGATE_RESULTS,
    ]
    task_queue = TaskQueue.IO
    dependencies = "passport-detection.io"
    worker_ctx = dev_worker_context(
        "test-passport-io-worker",
        is_async=True,
        activities=acts,
        dependencies=dependencies,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def preprocessing_worker(
    test_worker_config: PassportWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    acts = [Activity.PREPROCESS_IMAGES]
    task_queue = TaskQueue.PREPROCESSING
    dependencies = "passport-detection.preprocessing"
    worker_ctx = dev_worker_context(
        "test-passport-preprocessing-worker",
        is_async=False,
        activities=acts,
        dependencies=dependencies,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def inference_worker(
    test_worker_config: PassportWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    acts = [Activity.DETECT_PASSPORTS]
    task_queue = TaskQueue.INFERENCE
    dependencies = "passport-detection.inference"
    worker_ctx = dev_worker_context(
        "test-passport-preprocessing-worker",
        is_async=True,
        activities=acts,
        dependencies=dependencies,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
def test_model_path() -> Path:
    model_filename = "test_model_v0.onnx"
    test_model_path = TEST_RESOURCE_PATH / "models" / model_filename
    if not test_model_path.exists():
        raise ValueError("place a model in here or implement automatic model DL")
    return test_model_path
