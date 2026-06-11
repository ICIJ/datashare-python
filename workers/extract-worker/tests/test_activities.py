import json
import shutil
from collections.abc import AsyncGenerator, Iterable
from pathlib import Path
from typing import Any

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import DocumentLocation
from datashare_python.utils import read_jsonl
from extract_core import InputDoc, OutputFormat, Pipeline, Result, Status
from extract_core.objects import ConversionOutput, Error, PageIndexes, SupportedExt
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
    ErrorReport,
    MarkdownExtractResponse,
    ProcessedDoc,
    ProcessingReport,
)
from icij_common.es import ESClient, ids_query, match_all
from icij_common.registrable import FromConfig, RegistrableConfig

from tests import DOCS_PATH


class MockPipeline(Pipeline):
    def __init__(self, results: list[Result]) -> None:
        self._results = results

    async def extract_content(
        self,
        docs: Iterable[InputDoc],  # noqa: ARG002
        output_format: OutputFormat,  # noqa: ARG002
        output_path: Path,  # noqa: ARG002
    ) -> AsyncGenerator[Result, None]:
        for res in self._results:
            if res.status == Status.SUCCESS:
                shutil.copytree(DOCS_PATH / "markdown", output_path / res.output.path)
            yield res

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig: ...


PROCESSED_DOC_0 = ProcessedDoc(
    id="doc-0",
    path=Path(TEST_PROJECT, "symlinks", "do", "c-", "doc-0", "doc-0.pdf"),
    index=TEST_PROJECT,
    location=DocumentLocation.WORKDIR,
    resource_name="doc-0.pdf",
    n_pages=2,
)
PROCESSED_DOC_2 = ProcessedDoc(
    id="doc-2",
    path=Path("doc-2.docx"),
    index=TEST_PROJECT,
    location=DocumentLocation.ORIGINAL,
    resource_name="doc-2.docx",
    n_pages=1,
)


@pytest.mark.parametrize(
    ("docs", "expected_batches"),
    [
        # Supports empty query
        ({}, [[PROCESSED_DOC_0], [PROCESSED_DOC_2]]),
        # Return all supported docs
        (match_all(), [[PROCESSED_DOC_0], [PROCESSED_DOC_2]]),
        (ids_query(["doc-0"]), [[PROCESSED_DOC_0]]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_create_markdown_extraction_batches_act(
    docs_with_cached_artifacts: list[ProcessedDoc],  # noqa: ARG001
    test_es_client: ESClient,
    docs: list[DocId] | DocumentSearchQuery | None,
    expected_batches: list[tuple[DocId, Path]],
    tmpdir: Path,
) -> None:
    # Given
    tmpdir = Path(tmpdir)
    artifacts_root = tmpdir / "artifacts"
    workdir = tmpdir / "workdir"
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
            artifacts_root=artifacts_root,
            workdir=workdir,
            output_dir=tmpdir,
            target_n_pages_per_batch=target_n_pages_per_batch,
            es_client=client,
        )
    ]
    # Then
    results = []
    for b in batch_paths:
        results.append(
            [ProcessedDoc.model_validate(fs_doc) for fs_doc in read_jsonl(b)]
        )
    assert results == expected_batches


_RES_0 = Result(
    input=InputDoc(ext=SupportedExt.PDF, path=Path("doc-0.pdf")),
    status=Status.SUCCESS,
    output=ConversionOutput(path=Path("markdown"), pages=PageIndexes(root=[(0, 1)])),
)

_RES_2_ERRORS = [Error(id="error-id", title="error-title", detail="error-detail")]
_RES_2 = Result(
    input=InputDoc(ext=SupportedExt.DOCX, path=Path("doc-2.docx")),
    status=Status.FAILURE,
    output=None,
    errors=_RES_2_ERRORS,
)


async def test_extract_markdown_content_act(
    test_worker_config: ExtractWorkerConfig,
) -> None:
    # Given
    batch = [PROCESSED_DOC_0, PROCESSED_DOC_2]
    extract_results = [_RES_0, _RES_2]
    pipeline = MockPipeline(extract_results)
    workdir = test_worker_config.workdir
    output_dir = workdir / "output_dir"
    output_dir.mkdir()

    batch_path = workdir / "0.jsonl"
    with batch_path.open("w") as f:
        for doc in batch:
            f.write(f"{doc.model_dump_json()}\n")

    # When
    res = await extract_markdown_content_act(
        pipeline,
        batch=batch_path,
        worker_config=test_worker_config,
        output_dir=output_dir,
    )
    # Then
    errors = ErrorReport(
        doc=PROCESSED_DOC_2, status=Status.FAILURE, errors=_RES_2_ERRORS
    )
    expected_res = MarkdownExtractResponse(
        processed=ProcessingReport(n_docs=2, n_pages=3),
        successes=ProcessingReport(n_docs=1, n_pages=2),
        errors=[errors],
    )
    assert res == expected_res
    artifacts_root = test_worker_config.artifacts_root
    d = artifacts_root / TEST_PROJECT / "do" / "c-" / "doc-0"
    assert d.exists()
    assert d.is_dir()
    meta_path = d / "metadata.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    md_dir = meta.get("extract.markdown")
    assert md_dir is not None
    md_dir = d / md_dir
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
    ext_to_mime_types(SupportedExt.POTX)
    for ext in SupportedExt:
        # When
        mime_types = ext_to_mime_types(ext)
        # Then
        assert isinstance(mime_types, set)
