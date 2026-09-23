import json
import shutil
from collections.abc import AsyncIterable, Iterable
from pathlib import Path
from typing import Any

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    ArtifactType,
    DatashareFile,
    DatashareLanguage,
    DocProcessingErrors,
    Document,
    Error,
    ErrorReportWithPages,
    ManifestEntryStatus,
    ProcessedFile,
    ProcessingReportWithPages,
    WorkerFile,
)
from datashare_python.utils import read_jsonl_as
from extract_core import InputDoc, OutputFormat, Pipeline, Result, Status
from extract_core.objects import ConversionOutput, Pages, SupportedExt
from extract_core.objects import Error as ExtractCoreError
from extract_worker.activities import (
    _build_doc_query,
    create_markdown_extract_batches_act,
    ext_to_mime_types,
    extract_markdown_content_act,
)
from extract_worker.config import ExtractWorkerConfig
from extract_worker.objects import (
    DocId,
    DocumentSearchQuery,
    MarkdownExtractArgs,
    MarkdownExtractResponse,
    StructureManifestEntry,
)
from icij_common.es import ESClient, ids_query, match_all
from icij_common.registrable import FromConfig, RegistrableConfig

from tests import DOCS_PATH


class MockPipeline(Pipeline):
    def __init__(self, results: list[ConversionOutput | list[Error]]) -> None:
        self._results = iter(results)

    async def extract_content(
        self,
        docs: Iterable[InputDoc],  # noqa: ARG002
        output_format: OutputFormat,  # noqa: ARG002
        output_path: Path,  # noqa: ARG002
    ) -> AsyncIterable[Result]:
        for doc in docs:
            res = next(self._results)
            match res:
                case list():
                    yield Result(
                        input=doc, status=Status.FAILURE, output=None, errors=res
                    )
                case ConversionOutput():
                    shutil.copytree(DOCS_PATH / "markdown", output_path / res.path)
                    yield Result(input=doc, status=Status.SUCCESS, output=res)
                case _:
                    raise TypeError(f"unexpected result type {res}")

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig: ...


DOC_0 = Document(
    id="doc-0",
    root_document="root-0",
    path=Path("root-0.eml"),
    language=DatashareLanguage("ENGLISH"),
    index=TEST_PROJECT,
    metadata={
        TIKA_METADATA_RESOURCENAME: "doc-0.pdf",
        "tika_metadata_xmptpg_npages": 2,
    },
    extraction_level=1,
)

DOC_2 = Document(
    id="doc-2",
    root_document="doc-2",
    path=Path("doc-2.docx"),
    language=DatashareLanguage("ENGLISH"),
    index=TEST_PROJECT,
    metadata={
        TIKA_METADATA_RESOURCENAME: "doc-2.docx",
        "tika_metadata_xmptpg_npages": 1,
    },
)


@pytest.mark.parametrize(
    ("docs", "expected_batches"),
    [
        # Supports empty query
        ({}, [[(WorkerFile, "doc-0")], [(DatashareFile, "doc-2")]]),
        # Return all supported docs
        (match_all(), [[(WorkerFile, "doc-0")], [(DatashareFile, "doc-2")]]),
        (ids_query(["doc-0"]), [[(WorkerFile, "doc-0")]]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_create_markdown_extraction_batches_act(  # noqa: PLR0917
    docs_with_cached_artifacts: list[ProcessedFile],  # noqa: ARG001
    test_worker_config: ExtractWorkerConfig,
    test_es_client: ESClient,
    docs: list[DocId] | DocumentSearchQuery | None,
    expected_batches: list[list[tuple[type, DocId]]],
    tmpdir: Path,
) -> None:
    # Given
    paths = test_worker_config.paths
    tmpdir = Path(tmpdir)
    target_n_pages_per_batch = 1
    client = test_es_client
    supported_exts = {SupportedExt.PDF, SupportedExt.DOCX}
    # When
    batch_paths = [
        batch
        async for batch in create_markdown_extract_batches_act(
            docs,
            TEST_PROJECT,
            supported_exts,
            paths,
            output_dir=tmpdir,
            target_n_pages_per_task=target_n_pages_per_batch,
            es_client=client,
        )
    ]
    # Then
    results = []
    for b in batch_paths:
        results.append(list(read_jsonl_as(b, PROCESSED_FILE_TA)))
    types = [[type(i) for i in batch] for batch in results]
    expected_types = [[t for t, _ in batch] for batch in expected_batches]
    assert types == expected_types
    expected_ids = [[i for _, i in batch] for batch in expected_batches]
    ids = [[i.doc_id for i in batch] for batch in results]
    assert ids == expected_ids


_RES_0 = ConversionOutput(
    path=Path("markdown"),
    pages=Pages(total=2, byte_ranges=[(0, 1), (1, 2)]),
    confidence=None,
)
_RES_2 = [ExtractCoreError(id="error-id", title="error-title", detail="error-detail")]


async def test_extract_markdown_content_act(
    test_worker_config: ExtractWorkerConfig,
) -> None:
    # Given
    paths = test_worker_config.paths
    args = MarkdownExtractArgs(project=TEST_PROJECT, docs=[])
    symlink_path = paths.workdir.joinpath(
        TEST_PROJECT, "symlinks", "do", "-c", "doc-0", "doc-0.pdf"
    )
    symlinked_doc_0 = WorkerFile.from_parent(
        DatashareFile.from_parent(DOC_0), symlink_path, paths
    )
    doc_2 = DatashareFile.from_parent(DOC_2)
    batch = [symlinked_doc_0, doc_2]
    extract_results = [_RES_0, _RES_2]
    pipeline = MockPipeline(extract_results)
    workdir = paths.workdir
    output_dir = workdir / "output_dir"
    output_dir.mkdir()

    batch_path = workdir / "0.jsonl"
    with batch_path.open("w") as f:
        for doc in batch:
            f.write(f"{doc.model_dump_json()}\n")

    # When
    res = await extract_markdown_content_act(
        pipeline,
        args=args,
        batch=batch_path,
        worker_config=test_worker_config,
        output_dir=output_dir,
    )
    # Then
    expected_errors = Error(title=_RES_2[0].title, detail=None)
    errors = ErrorReportWithPages(
        n_docs=1,
        n_pages=1,
        errors=[
            DocProcessingErrors(
                doc_id=doc_2.doc_id,
                project=doc_2.project,
                root_document=doc_2.root_document,
                errors=[expected_errors],
            )
        ],
    )
    expected_res = MarkdownExtractResponse(
        processed=ProcessingReportWithPages(n_docs=2, n_pages=3),
        successes=ProcessingReportWithPages(n_docs=1, n_pages=2),
        errors=errors,
    )
    assert res == expected_res
    artifacts_path = test_worker_config.paths.artifacts
    d = artifacts_path / TEST_PROJECT / "do" / "c-" / "doc-0"
    assert d.exists()
    assert d.is_dir()
    meta_path = d / "manifest.json"
    assert meta_path.exists()
    manifest = json.loads(meta_path.read_text())
    entry = StructureManifestEntry.model_validate(
        manifest[ArtifactType.STRUCTURE.value]
    )
    assert entry.status is ManifestEntryStatus.COMPLETE
    assert entry.pages.pagination.byte_ranges
    assert entry.pages.total == 2
    md_dir = d / "structure"
    assert md_dir.exists()
    assert md_dir.is_dir()


_DEFAULT_PDF_QUERY = {
    "query": {
        "bool": {
            "must": [
                {"terms": {"contentType": ["application/pdf"]}},
                {"term": {"type": "Document"}},
            ]
        }
    }
}

_EXPECTED_TERM_QUERY = {
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "must": [
                            {"terms": {"contentType": ["application/pdf"]}},
                            {"term": {"type": "Document"}},
                        ]
                    }
                },
                {"term": {"some": "query"}},
            ]
        }
    }
}


_EXPECTED_BY_ID_QUERY = {
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "must": [
                            {"terms": {"contentType": ["application/pdf"]}},
                            {"term": {"type": "Document"}},
                        ]
                    }
                },
                {"ids": {"values": ["doc-id-0"]}},
            ]
        }
    }
}


@pytest.mark.parametrize(
    ("docs", "expected_query"),
    [
        (None, _DEFAULT_PDF_QUERY),
        ({"term": {"some": "query"}}, _EXPECTED_TERM_QUERY),
        (["doc-id-0"], _EXPECTED_BY_ID_QUERY),
    ],
)
def test_build_doc_query(
    docs: list[DocId] | DocumentSearchQuery | None, expected_query: dict[str, Any]
) -> None:
    # Given
    supported_formats = {SupportedExt.PDF}
    # When
    query = _build_doc_query(docs, supported_formats)
    # Then
    assert query == expected_query


@pytest.mark.parametrize(
    ("ext", "expected_mime_types"),
    [
        (SupportedExt.PDF, {"application/pdf"}),
        (
            SupportedExt.DOCX,
            {"application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
        ),
        (SupportedExt.JPG, {"image/jpg", "image/jpeg"}),
    ],
)
def test_ext_to_mime_type(ext: SupportedExt, expected_mime_types: set[str]) -> None:
    # When
    supported_mime_types = ext_to_mime_types(ext)
    # Then
    assert supported_mime_types == expected_mime_types


def test_all_supported_ext_should_have_mime_type() -> None:
    # Given
    for ext in SupportedExt:
        # When
        mtypes = ext_to_mime_types(ext)
        # Then
        assert isinstance(mtypes, set)
