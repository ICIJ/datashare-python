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
    ESClientConfig,
    LogFormat,
    LoggingConfig,
    TemporalClientConfig,
)
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    clear_dirs,
    dev_worker_context,
    index_docs,
    indexed_docs,
    pytest_collection_modifyitems,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client,
    test_temporal_client_session,
    text_0,
    text_1,
)
from datashare_python.objects import (
    ByteRangesPagination,
    DatashareFile,
    DatashareLanguage,
    Document,
    Pages,
    ProcessedFile,
    StructureManifestEntry,
    WorkerFile,
    WorkerPaths,
)
from datashare_python.types_ import TemporalClient
from datashare_python.utils import artifacts_dir, write_artifact
from docling.document_converter import DocumentConverter
from docling_core.transforms.chunker import HybridChunker
from extract_core import BatchConcurrencySettings, DoclingSettings
from extract_core.docling_ import InferenceSettings
from extract_core.objects import SupportedExt
from icij_common.es import ESClient

from extract_worker.activities import Activity
from extract_worker.config import (
    DoclingWorkerConfig,
    EmbeddingWorkerConfig,
    ExtractWorkerConfig,
    MarkdownExtractWorkerConfig,
    MarkdownInferenceWorkerConfig,
)
from extract_worker.constants import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_MODEL_REVISION,
    TaskQueue,
)
from extract_worker.embeddings import HFEmbedder, make_chunker, make_md_converter
from extract_worker.objects import MarkdownExtractArgs, StructureArtifact
from tests import DOCS_PATH

mimetypes.init()


TEST_EMBEDDING_DIM = 64

PAGE_SEP = '\n<div style="page-break-after: always;"></div>\n'

DOC_0_PAGES = [
    "# Offshore leaks\n\nShell companies registered in the islands.\n",
    "## Beneficial owners\n\nThe owners were hidden behind nominees.\n",
]

DOC_3_CONTENT = "The vote is on Sunday.\n\n  \n\nPolls close at eight.\n"


@pytest.fixture(scope="session")
def test_worker_config(tmp_path_factory: TempPathFactory) -> ExtractWorkerConfig:  # noqa: ANN001, ARG001, F811
    tmp_path = Path(tmp_path_factory.mktemp("test-"))
    filesystem = tmp_path / "docs"
    filesystem.mkdir()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    worker_paths = WorkerPaths(
        filesystem=filesystem, artifacts=artifacts, workdir=workdir
    )
    logging_config = LoggingConfig(
        loggers={datashare_python.__name__: "INFO", __name__: "DEBUG"},
        format=LogFormat.DEFAULT,
    )

    es_config = ESClientConfig(default_page_size=1)
    return ExtractWorkerConfig(
        elasticsearch=es_config,
        logging=logging_config,
        markdown=MarkdownExtractWorkerConfig(
            inference=MarkdownInferenceWorkerConfig(
                default_target_n_pages_per_task=1,
                docling=DoclingWorkerConfig(
                    settings=DoclingSettings(
                        perf=BatchConcurrencySettings(
                            max_page_batches=1, page_batch_size=1
                        ),
                        inference=InferenceSettings(document_timeout=120),
                    ),
                ),
            )
        ),
        embedding=EmbeddingWorkerConfig(max_tokens=128),
        datashare=DatashareClientConfig(url="http://localhost:8080"),
        temporal=TemporalClientConfig(host="localhost:7233"),
        paths=worker_paths,
    )


@pytest.fixture(scope="session")
def doc_0() -> Document:
    return Document(
        id="doc-0",
        root_document="root-0",
        extraction_level=1,
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
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
        extraction_level=1,
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
        content_type="audio/wav",
        path=Path("doc-1.wav"),
        metadata={"tika_metadata_resourcename": "doc-1.wav"},
    )


@pytest.fixture(scope="session")
def doc_2() -> Document:
    return Document(
        id="doc-2",
        index=TEST_PROJECT,
        language=DatashareLanguage("FRENCH"),
        content_type=mimetypes.guess_type("doc-2.docx")[0],
        path=Path("doc-2.docx"),
        metadata={"tika_metadata_resourcename": "doc-2.docx"},
    )


@pytest.fixture(scope="session")
def doc_3() -> Document:
    return Document(
        id="doc-3",
        root_document="root-3",
        extraction_level=1,
        index=TEST_PROJECT,
        language=DatashareLanguage("ENGLISH"),
        content=DOC_3_CONTENT,
        content_type="message/rfc822",
        path=Path("doc-3.eml"),
        metadata={"tika_metadata_resourcename": "doc-3.eml"},
    )


@pytest.fixture
async def populate_es(
    test_es_client: ESClient,  # noqa: F811
    doc_0: Document,
    doc_1: Document,
    doc_2: Document,
    doc_3: Document,
) -> list[Document]:
    docs = [doc_0, doc_1, doc_2, doc_3]
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs


@pytest.fixture
def docs_with_cached_artifacts(
    populate_es: list[Document],
    test_worker_config: ExtractWorkerConfig,
) -> list[ProcessedFile]:
    config = test_worker_config
    clear_dirs(test_worker_config)
    supported_exts = {SupportedExt.PDF, SupportedExt.DOCX}
    docs = [
        d for d in populate_es if d.path is not None and d.path.suffix in supported_exts
    ]
    files = []
    for doc in docs:
        doc_path = DOCS_PATH / doc.path
        if doc.is_root_document:
            config.paths.filesystem.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, config.paths.filesystem / doc.path)
        else:
            artifact_path = (
                config.paths.artifacts
                / artifacts_dir(doc.id, project=doc.project)
                / "raw"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(doc_path, artifact_path)
        ds_file = DatashareFile.from_parent(doc)
        files.append(ds_file)
    return files


def _write_structure_artifact(
    processed: DatashareFile, md_path: Path, pages: Pages, paths: WorkerPaths
) -> None:
    args = MarkdownExtractArgs(project=processed.project, docs=None)
    artifact = StructureArtifact(
        project=processed.project,
        doc_id=processed.doc_id,
        artifact=md_path,
        manifest_entry=StructureManifestEntry.complete(args=args, pages=pages),
    )
    write_artifact(paths.artifacts, artifact)


@pytest.fixture
def doc_with_structure(
    doc_0: Document, test_worker_config: ExtractWorkerConfig
) -> DatashareFile:
    clear_dirs(test_worker_config)
    paths = test_worker_config.paths
    md_path, pages = _write_md(doc_0.id, DOC_0_PAGES, paths)
    ds_file = DatashareFile.from_parent(doc_0)
    _write_structure_artifact(ds_file, md_path, pages, paths)
    return ds_file


@pytest.fixture
def doc_without_md(
    doc_3: Document, test_worker_config: ExtractWorkerConfig
) -> WorkerFile:
    paths = test_worker_config.paths
    content_path = paths.workdir / f"{doc_3.id}-content"
    content_path.write_text(doc_3.content)
    return WorkerFile.from_parent(
        DatashareFile.from_parent(doc_3), paths=paths, path=content_path
    )


def _write_md(doc_id: str, pages: list[str], paths: WorkerPaths) -> tuple[Path, Pages]:
    contents = [(p + PAGE_SEP).encode() for p in pages[:-1]] + [pages[-1].encode()]
    byte_ranges, start = [], 0
    for c in contents:
        byte_ranges.append((start, start + len(c)))
        start += len(c)
    md_dir = paths.workdir / doc_id
    md_dir.mkdir(exist_ok=True, parents=True)
    (md_dir / "doc.md").write_bytes(b"".join(contents))
    pages = Pages(
        total=len(contents),
        pagination=ByteRangesPagination(byte_ranges=byte_ranges),
    )
    return md_dir, pages


@pytest.fixture(scope="session")
async def workflows_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-workflows-worker-{uuid.uuid4()}"
    task_queue = TaskQueue.WORKFLOWS
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        workflows=["extract.markdown", "extract.embed"],
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def io_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-io-worker-{uuid.uuid4()}"
    acts = [
        Activity.WORKER_CONFIG,
        Activity.CREATE_MD_BATCHES,
        Activity.CHECK_EMBEDDINGS_INDEX,
        Activity.CREATE_EMBEDDING_BATCHES,
        Activity.INDEX_EMBEDDINGS,
    ]
    task_queue = TaskQueue.IO
    dependencies = "extract.io"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=acts,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def md_extract_cpu_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None]:
    client = test_temporal_client_session
    worker_id = f"test-extract-cpu-worker-{uuid.uuid4()}"
    acts = [Activity.EXTRACT_MD_CONTENT]
    task_queue = TaskQueue.EXTRACT_CPU
    dependencies = "extract.extract"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=acts,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
async def embed_cpu_worker(
    test_worker_config: ExtractWorkerConfig,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[None]:
    client = test_temporal_client_session
    worker_id = f"test-embed-cpu-worker-{uuid.uuid4()}"
    acts = [Activity.EMBED_DOCUMENTS]
    task_queue = TaskQueue.EMBED_CPU
    dependencies = "extract.embed"
    worker_ctx = dev_worker_context(
        worker_id,
        is_async=True,
        activities=acts,
        worker_config=test_worker_config,
        client=client,
        task_queue=task_queue,
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture(scope="session")
def test_embedding_config() -> EmbeddingWorkerConfig:
    return EmbeddingWorkerConfig()


@pytest.fixture(scope="session")
def test_embedder(test_embedding_config: EmbeddingWorkerConfig) -> HFEmbedder:
    return HFEmbedder(
        test_embedding_config,
        model=DEFAULT_EMBEDDING_MODEL,
        model_revision=DEFAULT_EMBEDDING_MODEL_REVISION,
    )


@pytest.fixture(scope="session")
def test_chunker(test_embedding_config: EmbeddingWorkerConfig) -> HybridChunker:
    return make_chunker(
        test_embedding_config,
        model=DEFAULT_EMBEDDING_MODEL,
        model_revision=DEFAULT_EMBEDDING_MODEL_REVISION,
        merge_peers=False,
    )


@pytest.fixture(scope="session")
def test_md_converter() -> DocumentConverter:
    return make_md_converter()


@pytest.fixture(scope="session")
def chunk_index(test_worker_config: ExtractWorkerConfig) -> str:
    return test_worker_config.embedding.index_name(TEST_PROJECT)
