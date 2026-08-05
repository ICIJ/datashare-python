from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document, ProcessedFile, ProcessedPage
from datashare_python.utils import safe_dir
from icij_common.pydantic_utils import safe_copy
from icij_common.registrable import FromConfig, RegistrableConfig
from passport_service.constants import GOTENBERG_SUPPORTED_EXTS
from passport_service.exceptions import InvalidPDF, UnsupportedDocExtension
from passport_worker.config import PassportWorkerConfig
from passport_worker.preprocessing import (
    DefaultImagePreprocessor,
    ImagePreprocessor,
    PDFConverter,
    PDFPreprocessor,
    convert_to_pdfs_act,
    preprocess_images_act,
    preprocess_pdfs_act,
)
from PIL import Image

from tests import DOCS_PATH
from tests.conftest import (
    PROCESSED_DOC_0,
    PROCESSED_DOC_1,
    PROCESSED_DOC_2,
    SYMLINKED_PROCESSED_DOC_0,
)


class MockImageProcessor(ImagePreprocessor):
    def __init__(self, res: list[list[Path] | Exception]):
        self._res = iter(res)

    def __call__(self, image_path: Path, *, output_dir: Path) -> list[Path]:  # noqa: ARG002
        r = next(self._res)
        if isinstance(r, Exception):
            raise r
        return r

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig: ...


class MockConverter(PDFConverter):
    def __init__(self, conversion_results: dict[str, bytes | Exception]):
        self._conversion_results = conversion_results

    async def __call__(self, doc: ProcessedFile, doc_bytes: bytes) -> bytes:  # noqa: ARG002
        res = self._conversion_results[doc.id]
        if isinstance(res, Exception):
            raise res
        return res

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig:
        pass


class MockPDFPreprocessor(PDFPreprocessor):
    def __init__(self, res: list[list[Path] | Exception]):
        self._res = iter(res)

    def __call__(
        self,
        pdf_path: Path,  # noqa: ARG002
        pdf_bytes: bytes,  # noqa: ARG002
        output_dir: Path,  # noqa: ARG002
    ) -> list[Path]:
        r = next(self._res)
        if isinstance(r, Exception):
            raise r
        return r


@pytest.fixture
def symlinked_doc_0() -> ProcessedFile:
    # Let's test with a symlinked file
    symlink_path = Path(TEST_PROJECT, "symlinks", "do", "c-", "not_a_passport.jpg")
    symlinked_doc = safe_copy(PROCESSED_DOC_0, {"path": symlink_path})
    return symlinked_doc


@pytest.fixture
def symlinked_doc_0_pages(
    test_worker_config: PassportWorkerConfig, symlinked_doc_0: ProcessedFile
) -> list[Path]:
    worker_paths = test_worker_config.paths
    workdir = worker_paths.workdir
    output_root = workdir.joinpath("workflow_id")
    output_root.mkdir(parents=True, exist_ok=True)
    doc_0_page_dir = output_root / safe_dir(symlinked_doc_0.id) / symlinked_doc_0.id
    doc_0_pages = [doc_0_page_dir / "page_1.png", doc_0_page_dir / "page_2.png"]
    return doc_0_pages


def test_preprocess_images_act(
    test_worker_config: PassportWorkerConfig,
    symlinked_doc_0_pages: list[Path],
) -> None:
    # Given
    config = test_worker_config
    executor = test_worker_config.to_image_preprocessing_executor()
    worker_paths = config.paths
    workdir = worker_paths.workdir
    output_root = workdir.joinpath("workflow_id")
    output_root.mkdir(parents=True, exist_ok=True)
    doc_0_pages = symlinked_doc_0_pages
    processor = MockImageProcessor([doc_0_pages])
    batch_path = output_root / "batch.jsonl"
    batch = [SYMLINKED_PROCESSED_DOC_0, PROCESSED_DOC_1]
    batch_path.write_text("\n".join(d.model_dump_json() for d in batch))

    # When
    successes, errors = preprocess_images_act(
        batch_path,
        worker_paths,
        output_root=output_root,
        executor=executor,
        image_preprocessor=processor,
    )

    # Then
    expected_successes = [
        ProcessedPage(
            page_number=page_number + 1,
            **SYMLINKED_PROCESSED_DOC_0.child(p, worker_paths).model_dump(),
        )
        for page_number, p in enumerate(doc_0_pages)
    ]
    assert successes == expected_successes
    assert len(errors) == 1
    processing_error = errors[0]
    assert processing_error.file.id == PROCESSED_DOC_1.id
    assert processing_error.error.title == "UnsupportedDocExtension"


async def test_convert_to_pdfs_act(
    test_worker_config: PassportWorkerConfig,
    docs_with_cached_artifacts: list[ProcessedFile],  # noqa: ARG001
) -> None:
    # Given
    config = test_worker_config
    worker_paths = config.paths
    workdir = worker_paths.workdir
    output_root = workdir.joinpath("workflow_id")
    output_root.mkdir(parents=True)
    max_concurrency = 1
    batch = [PROCESSED_DOC_2, PROCESSED_DOC_1]
    conversion_results = {
        PROCESSED_DOC_2.id: b"doc_2_as_pdf",
        PROCESSED_DOC_1.id: UnsupportedDocExtension(
            ".weirdext", sorted(GOTENBERG_SUPPORTED_EXTS)
        ),
    }
    gotenberg_client = MockConverter(conversion_results=conversion_results)
    batch_path = output_root / "batch.jsonl"
    batch_path.write_text("\n".join(d.model_dump_json() for d in batch))
    # When
    successes, errors = await convert_to_pdfs_act(
        batch_path,
        gotenberg_client,
        worker_paths,
        max_concurrency=max_concurrency,
        output_root=output_root,
    )
    # Then
    doc_2_as_pdf_path = (
        output_root / safe_dir(PROCESSED_DOC_2.id) / f"{PROCESSED_DOC_2.id}.pdf"
    )
    expected_successes = [PROCESSED_DOC_2.child(doc_2_as_pdf_path, worker_paths)]
    assert successes == expected_successes
    assert len(errors) == 1
    processing_error = errors[0]
    assert processing_error.file.id == PROCESSED_DOC_1.id
    assert processing_error.error.title == "UnsupportedDocExtension"


@pytest.fixture
def doc_1_pages(
    test_worker_config: PassportWorkerConfig, doc_0: Document
) -> list[Path]:
    worker_paths = test_worker_config.paths
    workdir = worker_paths.workdir
    output_root = workdir.joinpath("workflow_id")
    output_root.mkdir(parents=True, exist_ok=True)
    doc_0_page_dir = output_root / safe_dir(doc_0.id) / doc_0.id
    doc_0_pages = [doc_0_page_dir / "page_0.png", doc_0_page_dir / "page_1.png"]
    return doc_0_pages


async def test_preprocess_pdfs_act(
    test_worker_config: PassportWorkerConfig,
    doc_1_pages: list[Path],
    docs_with_cached_artifacts: list[ProcessedFile],  # noqa: ARG001
) -> None:
    # Given
    config = test_worker_config
    worker_paths = config.paths
    workdir = worker_paths.workdir
    output_root = workdir.joinpath("workflow_id")
    output_root.mkdir(parents=True, exist_ok=True)
    batch = [PROCESSED_DOC_1, PROCESSED_DOC_0]
    results = [doc_1_pages, InvalidPDF(PROCESSED_DOC_0.id)]
    preprocessor = MockPDFPreprocessor(results)
    batch_path = output_root / "pdfs.jsonl"
    batch_path.write_text("\n".join(d.model_dump_json() for d in batch))
    # When
    successes, errors = await preprocess_pdfs_act(
        batch_path,
        worker_paths,
        preprocessor,
        output_root=output_root,
    )
    # Then
    expected_successes = [
        ProcessedPage(
            page_number=page_number + 1,
            **PROCESSED_DOC_1.child(p, worker_paths).model_dump(),
        )
        for page_number, p in enumerate(doc_1_pages)
    ]
    assert successes == expected_successes
    assert len(errors) == 1
    processing_error = errors[0]
    assert processing_error.file.id == PROCESSED_DOC_0.id
    assert processing_error.error.title == "InvalidPDF"


def test_default_image_preprocessor(tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    im_path = DOCS_PATH / "not_a_passport.jpg"
    preprocessor = DefaultImagePreprocessor()
    # When
    paths = preprocessor(im_path, output_dir=output_dir)
    assert len(paths) == 1
    processed_path = paths[0]
    assert processed_path.name.endswith(".png")
    im = Image.open(processed_path)
    assert im.mode == "RGB"
