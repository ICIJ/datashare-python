from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import BaseModel
from datashare_python.utils import ext_to_mime_types, read_jsonl_as
from icij_common.es import ESClient, ids_query, match_all
from passport_service.constants import GOTENBERG_SUPPORTED_EXTS
from passport_service.core.preprocessing import (
    PIL_SUPPORTED_EXTENSIONS,
)
from passport_worker.config import PassportWorkerConfig
from passport_worker.objects import DocId, DocumentSearchQuery, ProcessedFile
from passport_worker.search import (
    create_preprocessing_batches_act,
    restrict_image_formats,
    restrict_to_pdf_file_formats,
)
from pydantic import Field

from tests.conftest import (
    PROCESSED_DOC_1,
    PROCESSED_DOC_2,
    PROCESSED_DOC_5,
    SYMLINKED_PROCESSED_DOC_0,
)


class _PreprocessingBatches(BaseModel):
    to_pdf: list[list[ProcessedFile]] = Field(default_factory=list)
    images: list[list[ProcessedFile]] = Field(default_factory=list)
    pdfs: list[list[ProcessedFile]] = Field(default_factory=list)


@pytest.mark.parametrize(
    ("docs", "expected_batches"),
    [
        # Supports empty query
        (
            {},
            _PreprocessingBatches(
                to_pdf=[[PROCESSED_DOC_2]],
                images=[[SYMLINKED_PROCESSED_DOC_0]],
                pdfs=[[PROCESSED_DOC_1], [PROCESSED_DOC_5]],
            ),
        ),
        # Return all supported docs
        (
            match_all(),
            _PreprocessingBatches(
                to_pdf=[[PROCESSED_DOC_2]],
                images=[[SYMLINKED_PROCESSED_DOC_0]],
                pdfs=[[PROCESSED_DOC_1], [PROCESSED_DOC_5]],
            ),
        ),
        (
            ids_query(["doc-0"]),
            _PreprocessingBatches(images=[[SYMLINKED_PROCESSED_DOC_0]]),
        ),
        # Should filter non supported content type
        (ids_query(["doc-6"]), _PreprocessingBatches()),
    ],
)
async def test_create_preprocessing_batches(
    test_worker_config: PassportWorkerConfig,
    docs_with_cached_artifacts: list[ProcessedFile],  # noqa: ARG001
    test_es_client: ESClient,
    docs: list[DocId] | DocumentSearchQuery | None,
    expected_batches: list[tuple[DocId, Path]],
    tmpdir: Path,
) -> None:
    # Given
    worker_paths = test_worker_config.paths
    tmpdir = Path(tmpdir)
    target_n_pages_per_batch = 1
    client = test_es_client
    supported_image_exts = {".jpg"}
    supported_doc_exts = {".docx"}
    # When
    batches = await create_preprocessing_batches_act(
        docs,
        TEST_PROJECT,
        client,
        worker_paths,
        target_n_pages_per_batch,
        supported_image_exts=supported_image_exts,
        supported_doc_exts=supported_doc_exts,
        output_root=tmpdir,
    )

    # Then
    to_pdf = []
    for p in batches.to_pdf:
        to_pdf.append(list(read_jsonl_as(p, ProcessedFile)))
    images = []
    for p in batches.images:
        images.append(list(read_jsonl_as(p, ProcessedFile)))
    pdfs = []
    for p in batches.pdfs:
        pdfs.append(list(read_jsonl_as(p, ProcessedFile)))
    batches = _PreprocessingBatches(to_pdf=to_pdf, images=images, pdfs=pdfs)
    assert batches.model_dump() == expected_batches.model_dump()


def test_restrict_image_formats() -> None:
    # When
    restricted = restrict_image_formats(PIL_SUPPORTED_EXTENSIONS)
    # Then
    for ext in sorted(restricted):
        types = ext_to_mime_types(ext)
        assert "application/octet-stream" not in types


def test_ext_to_mime_types_for_gotenberg_extensions() -> None:
    # When
    restricted = restrict_to_pdf_file_formats(GOTENBERG_SUPPORTED_EXTS)
    # Then
    for ext in sorted(restricted):
        types = ext_to_mime_types(ext)
        assert "application/octet-stream" not in types
