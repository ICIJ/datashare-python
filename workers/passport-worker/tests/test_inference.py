import json
import os
from collections.abc import Sequence
from pathlib import Path

import cv2
import numpy as np
import pytest
from cv2.typing import MatLike
from datashare_python.conftest import TEST_PROJECT
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    DatashareFile,
    DatashareLanguage,
    Document,
    FileLocation,
    WorkerRoots,
)
from datashare_python.utils import async_read_jsonl_as, safe_dir, write_batches
from icij_common.pydantic_utils import safe_copy
from icij_common.registrable import FromConfig, RegistrableConfig
from objects import ProcessingReportWithPages
from passport_service.objects import MRZ, ObjectDetection, Passport
from passport_worker.activities import Activity
from passport_worker.config import PassportWorkerConfig
from passport_worker.exceptions import InferenceRuntimeError
from passport_worker.inference import (
    PassportDetector,
    YOLOPassportDetector,
    create_inference_batches_act,
    detect_passports_act,
)
from passport_worker.objects import (
    PagePassports,
    PassportDetectionArgs,
    PassportDetectionConfig,
    PassportInferenceConfig,
    PassportManifestEntry,
    Passports,
    ProcessedPage,
    YOLOPassportDetectorConfig,
)
from passport_worker.preprocessing import PDFPreprocessor

from tests import DOCS_PATH
from tests.conftest import DS_FILE_0


class MockPassportDetector(PassportDetector):
    def __init__(
        self, detections: Sequence[list[list[ObjectDetection]]], mrzs: list[Passport]
    ):
        self._detections = iter(detections)
        self._mrzs = iter(mrzs)

    def detect_passports(
        self,
        ins: Sequence[tuple[MatLike, float]],  # noqa: ARG002
    ) -> list[list[ObjectDetection]]:
        return next(self._detections)

    def read_mrz(
        self,
        page: "np.array",  # noqa: ARG002
        passport: ObjectDetection,  # noqa: ARG002
        country_codes: list[str] | None = None,  # noqa: ARG002
    ) -> Passport:
        return next(self._mrzs)

    def scale_image(self, im: "MatLike") -> "tuple[np.ndarray, MatLike, float]":
        return np.zeros((640, 640, 3), np.uint8), im, 1.0

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> FromConfig:
        pass


class FailingPassportDetector:
    def __init__(self): ...
    def detect_passports(
        self,
        ins: Sequence[tuple[MatLike, float]],  # noqa: ARG002
    ) -> list[list[ObjectDetection]]:
        raise InferenceRuntimeError("i'm always failing")

    def scale_image(self, im: "MatLike") -> "tuple[np.ndarray, MatLike, float]":
        return np.zeros((640, 640, 3), np.uint8), im, 1.0

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
def symlinked_doc_0() -> DatashareFile:
    # Let's test with a symlinked file
    symlink_path = Path(TEST_PROJECT, "symlinks", "do", "c-", "not_a_passport.jpg")
    location = FileLocation(
        path=symlink_path,
        resource_name=symlink_path.name,
        location=FileLocation.ARTIFACTS,
    )
    symlinked_doc = safe_copy(DS_FILE_0, {"value": location})
    return symlinked_doc


@pytest.fixture
def symlinked_doc_0_pages(
    test_worker_config: PassportWorkerConfig, symlinked_doc_0: DatashareFile
) -> list[Path]:
    worker_paths = test_worker_config.roots
    workdir = worker_paths.workdir
    output_root = workdir.joinpath(TEST_PROJECT, "workflow_id")
    output_root.mkdir(parents=True)
    doc_0_page_dir = (
        output_root / safe_dir(symlinked_doc_0.doc_id) / symlinked_doc_0.doc_id
    )
    doc_0_pages = [doc_0_page_dir / "page_0.png", doc_0_page_dir / "page_1.png"]
    return doc_0_pages


@pytest.fixture
def doc_0_page_0(test_worker_config: PassportWorkerConfig) -> ProcessedPage:
    roots = test_worker_config.roots
    page_path = roots.workdir.joinpath(
        TEST_PROJECT,
        Activity.PREPROCESS_IMAGES,
        "activity-id",
        "doc-",
        "doc-0",
        "page_0.png",
    )
    page = ProcessedPage.from_parent(DS_FILE_0, page_path, roots, page=0)
    return page


@pytest.fixture
def doc_0_page_1(test_worker_config: PassportWorkerConfig) -> ProcessedPage:
    roots = test_worker_config.roots
    page_path = roots.workdir.joinpath(
        TEST_PROJECT,
        Activity.PREPROCESS_IMAGES,
        "activity-id",
        "doc-",
        "doc-0",
        "page_1.png",
    )
    page = ProcessedPage.from_parent(DS_FILE_0, page_path, roots, page=0)
    return page


@pytest.fixture
def doc_6_page_0(test_worker_config: PassportWorkerConfig) -> ProcessedPage:
    roots = test_worker_config.roots
    parent = DatashareFile.from_parent(
        Document(
            index=TEST_PROJECT,
            id="doc-6",
            root_document="doc-6",
            path="doc-6.png",
            metadata={TIKA_METADATA_RESOURCENAME: "doc-6.png"},
            language=DatashareLanguage("ENGLISH"),
            extraction_level=0,
            content_type="image/png",
        )
    )
    page_path = roots.workdir.joinpath(
        TEST_PROJECT,
        Activity.PREPROCESS_IMAGES,
        "activity-id",
        "doc-",
        "doc-6",
        "page_0.png",
    )
    page = ProcessedPage.from_parent(parent, page_path, roots, page=0)
    return page


@pytest.fixture
def doc_7_page_0(test_worker_config: PassportWorkerConfig) -> ProcessedPage:
    roots = test_worker_config.roots
    parent = DatashareFile.from_parent(
        Document(
            index=TEST_PROJECT,
            id="doc-7",
            root_document="doc-7",
            path="doc-7.png",
            metadata={TIKA_METADATA_RESOURCENAME: "doc-7.png"},
            language=DatashareLanguage("ENGLISH"),
            extraction_level=0,
            content_type="image/png",
        )
    )
    page_path = roots.workdir.joinpath(
        TEST_PROJECT,
        Activity.PREPROCESS_IMAGES,
        "activity-id",
        "doc-",
        "doc-7",
        "page_0.png",
    )
    page = ProcessedPage.from_parent(parent, page_path, roots, page=0)
    return page


async def test_create_inference_batches_act(
    test_worker_config: PassportWorkerConfig,
    doc_0_page_0: ProcessedPage,
    doc_0_page_1: ProcessedPage,
    doc_6_page_0: ProcessedPage,
    doc_7_page_0: ProcessedPage,
) -> None:
    # Given
    target_batches_per_task = 1
    inference_batch_size = 1
    config = test_worker_config
    worker_paths = config.roots
    workdir = worker_paths.workdir
    output_root = workdir.joinpath(TEST_PROJECT, "workflow_id")
    output_root.mkdir(parents=True, exist_ok=True)

    pages = [[doc_6_page_0], [doc_0_page_0, doc_0_page_1, doc_7_page_0]]
    batch_paths = []
    for batch_i, batch in enumerate(pages):
        batch_path = output_root / f"activity_{batch_i}.jsonl"
        batch_path.write_text("\n".join(d.model_dump_json() for d in batch))
        batch_paths.append(batch_path)

    # When
    batches = await create_inference_batches_act(
        batch_paths,
        worker_paths,
        output_root,
        target_batches_per_task=target_batches_per_task,
        inference_batch_size=inference_batch_size,
    )

    # Then
    batches = [
        [b async for b in async_read_jsonl_as(worker_paths.workdir / p, ProcessedPage)]
        for p in batches
    ]  # noqa: F821
    expected_batches = [
        [doc_6_page_0],
        [doc_0_page_0, doc_0_page_1],
        [doc_7_page_0],
    ]
    assert batches == expected_batches


_DOC_0_PAGE_0_DETECTION = ObjectDetection(
    class_id="passport", confidence=0.9, box=(1.0, 1.0, 1.0, 1.0)
)
_DOC_7_PAGE_0_DETECTION = ObjectDetection(
    class_id="passport", confidence=1.0, box=(1.0, 1.0, 1.0, 1.0)
)
_DOC_0_PAGE_0_PASSPORT = Passport.from_detection(
    _DOC_0_PAGE_0_DETECTION, mrz=MRZ(country="France", metadata={"name": "jean"})
)
_DOC_7_PAGE_0_PASSPORT = Passport.from_detection(_DOC_7_PAGE_0_DETECTION, None)
_EXPECTED_PASSPORTS_0 = Passports(
    pages=[PagePassports(page_number=0, passports=[_DOC_0_PAGE_0_PASSPORT])]
)
_EXPECTED_PASSPORTS_7 = Passports(
    pages=[PagePassports(page_number=0, passports=[_DOC_7_PAGE_0_PASSPORT])]
)


def _mock_pages(
    batch: list[ProcessedPage],
    roots: WorkerRoots,
    *,
    errors: list[ProcessedPage],
) -> None:
    for p in batch:
        page_path = p.locate(roots)
        page_path.parent.mkdir(parents=True, exist_ok=True)
        if not page_path.exists():
            os.symlink(DOCS_PATH / "passport.png", page_path)
    for error in errors:
        # Generate an error
        invalid_im_path = error.locate(roots)
        if invalid_im_path.exists():
            os.remove(invalid_im_path)
        os.symlink(DOCS_PATH / "passport.pdf", invalid_im_path)


async def test_detect_passports_act(  # noqa: PLR0917
    test_worker_config: PassportWorkerConfig,
    test_model_path: Path,
    doc_0_page_0: ProcessedPage,
    doc_0_page_1: ProcessedPage,
    doc_6_page_0: ProcessedPage,
    doc_7_page_0: ProcessedPage,
) -> None:
    # Given
    config = test_worker_config
    docs = [f"doc-{i}" for i in range(8)]
    worker_paths = config.roots
    args = PassportDetectionArgs(
        project=TEST_PROJECT,
        docs=docs,
        config=PassportDetectionConfig(
            inference=PassportInferenceConfig(
                passport_detector=YOLOPassportDetectorConfig(model_path=test_model_path)
            )
        ),
    )
    batch = [doc_6_page_0, doc_0_page_0, doc_0_page_1, doc_7_page_0]
    _mock_pages(batch, worker_paths, errors=[doc_0_page_1])
    batches = [b async for b in write_batches([batch], worker_paths.workdir)]
    batch = batches[0]
    detections = [[[]], [[_DOC_0_PAGE_0_DETECTION]], [[_DOC_7_PAGE_0_DETECTION]]]
    passport_detector = MockPassportDetector(
        detections, [_DOC_0_PAGE_0_PASSPORT, _DOC_7_PAGE_0_PASSPORT]
    )
    # When
    res = await detect_passports_act(
        batch, passport_detector, worker_paths, args, batch_size=1
    )
    # Then
    assert res.processed == ProcessingReportWithPages(n_docs=3, n_pages=4)
    assert res.successes == ProcessingReportWithPages(n_docs=2, n_pages=3)
    assert len(res.errors) == 1
    error = res.errors[0]
    assert error.source == doc_0_page_1
    assert error.error.title == "InvalidImage"
    expected = [
        ("doc-6", (PassportManifestEntry.complete(args), Passports())),
        (
            "doc-0",
            (PassportManifestEntry.partial(args), _EXPECTED_PASSPORTS_0),
        ),
        (
            "doc-7",
            (PassportManifestEntry.complete(args), _EXPECTED_PASSPORTS_7),
        ),
    ]
    for doc_id, written in expected:
        artifacts_path = (
            worker_paths.artifacts / TEST_PROJECT / safe_dir(doc_id) / doc_id
        )
        passports_path = artifacts_path / "passports.json"
        manifest_path = artifacts_path / "manifest.json"
        expected_manifest_entry, expected_passports = written
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        manifest_entry = PassportManifestEntry.model_validate(manifest["passports"])
        assert manifest_entry == expected_manifest_entry
        assert passports_path.exists()
        passports = Passports.model_validate_json(passports_path.read_text())
        assert passports == expected_passports


async def test_detect_passports_act_should_report_inference_failure(
    test_worker_config: PassportWorkerConfig,
    test_model_path: Path,
    doc_6_page_0: ProcessedPage,
) -> None:
    # Given
    config = test_worker_config
    docs = [f"doc-{i}" for i in range(8)]
    worker_paths = config.roots
    args = PassportDetectionArgs(
        project=TEST_PROJECT,
        docs=docs,
        config=PassportDetectionConfig(
            inference=PassportInferenceConfig(
                passport_detector=YOLOPassportDetectorConfig(model_path=test_model_path)
            )
        ),
    )
    batch = [doc_6_page_0]
    _mock_pages(batch, worker_paths, errors=[])
    batches = [b async for b in write_batches([batch], worker_paths.workdir)]
    batch = batches[0]
    passport_detector = FailingPassportDetector()
    # When
    res = await detect_passports_act(
        batch, passport_detector, worker_paths, args, batch_size=1
    )
    # Then
    assert res.processed == ProcessingReportWithPages(n_docs=1, n_pages=1)
    assert res.successes == ProcessingReportWithPages(n_docs=0, n_pages=0)
    assert len(res.errors) == 1
    error = res.errors[0]
    assert error.source == doc_6_page_0
    assert error.error.title == "InferenceRuntimeError"


TESTED_DOCS = sorted(
    f for f in DOCS_PATH.iterdir() if f.is_file() and f.suffix in {".jpg", ".png"}
)


@pytest.mark.e2e
@pytest.mark.parametrize(("doc_path"), TESTED_DOCS)
def test_yolo_passport_detector_e2e(doc_path: Path, test_model_path: Path) -> None:
    # Given
    config = YOLOPassportDetectorConfig(model_path=test_model_path)
    passport_detector = YOLOPassportDetector.from_config(config)  # noqa: F821
    im = cv2.imread(doc_path)
    # When
    with passport_detector:
        scaled, page, scale = passport_detector.scale_image(im)
        passport_pages = passport_detector.detect_passports([(page, scale)])
        passport_pages = passport_pages[0]
        for i_passport, passport_page in enumerate(passport_pages):
            passport_pages[i_passport] = passport_detector.read_mrz(
                scaled, passport_page
            )
    # Then
    expected_passport = "not" not in doc_path.name
    if expected_passport:
        assert len(passport_pages) == 2
        with_mrz = [
            passport_page
            for passport_page in passport_pages
            if passport_page.mrz is not None
        ]
        assert len(with_mrz) == 1
        with_mrz = with_mrz[0]
        assert with_mrz.mrz is not None
        assert with_mrz.mrz.metadata["names"] == "JANE"
    else:
        assert not passport_pages
