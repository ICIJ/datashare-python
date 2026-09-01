import asyncio
import csv
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, Iterable, Sequence
from functools import cache
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self

from aiofile import async_open
from datashare_python.objects import (
    ManifestEntryStatus,
    ProcessedFile,
    ProcessedPage,
    WorkerPaths,
)
from datashare_python.types_ import AsyncProgressRateHandler, RawAsyncProgressHandler
from datashare_python.utils import (
    async_read_jsonl_as,
    read_jsonl_as,
    to_incremental_async_progress,
    to_raw_async_progress,
    write_artifact,
)
from icij_common.iter_utils import async_batches
from icij_common.registrable import (
    RegistrableFromConfig,
)
from passport_service import DATA_DIR
from passport_service.exceptions import InvalidImage
from passport_service.objects import ObjectDetection, Passport

from passport_worker.exceptions import InferenceRuntimeError
from passport_worker.objects import (
    FileProcessingError,
    PagePassports,
    PartialDetectionResult,
    PassportArtifact,
    PassportDetectionArgs,
    PassportDetectorType,
    PassportManifestEntry,
    Passports,
    ProcessingReport,
    YOLOPassportDetectorConfig,
)
from passport_worker.utils import write_batches

if TYPE_CHECKING:
    import numpy as np
    from cv2.typing import MatLike

DetectionInputs = tuple["MatLike", float]

logger = logging.getLogger(__name__)


class PassportDetector(RegistrableFromConfig, ABC):
    @abstractmethod
    def detect_passports(
        self, ins: Sequence[DetectionInputs]
    ) -> list[list[ObjectDetection]]: ...

    @abstractmethod
    def read_mrz(
        self,
        page: "np.array",
        passport: ObjectDetection,
        country_codes: list[str] | None = None,
    ) -> Passport: ...

    @abstractmethod
    def scale_image(self, im: "MatLike") -> "tuple[np.ndarray, MatLike, float]": ...


_PASSPORT_CLASSES = ["passport"]


async def create_inference_batches_act(
    batches: list[Path],
    paths: WorkerPaths,
    output_root: Path,
    target_batches_per_task: int = 5,
    inference_batch_size: int = 16,
) -> list[Path]:
    batches = _inference_batches(
        batches,
        paths,
        inference_batch_size,
        target_batches_per_task=target_batches_per_task,
    )
    batch_paths = [
        b async for b in write_batches(batches, output_root, prefix="inference_batch_")
    ]
    return batch_paths


async def _inference_batches(
    batches: list[Path],
    paths: WorkerPaths,
    inference_batch_size: int,
    *,
    target_batches_per_task: int,
) -> AsyncIterable[list[ProcessedPage]]:
    pages = (
        d
        for p in batches
        async for d in async_read_jsonl_as(paths.workdir / p, ProcessedPage)
    )
    target_size = target_batches_per_task * inference_batch_size
    batch = []
    current_doc = None
    async for page in pages:
        if page.id != current_doc:
            # We prefer larger batches than smaller onces in order to saturate
            if len(batch) >= target_size:
                yield batch
                batch = []
            current_doc = page.id
        batch.append(page)
    if batch:
        yield batch


async def detect_passports_act(  # noqa: PLR0917
    batch: Path,
    passport_detector: PassportDetector,
    paths: WorkerPaths,
    args: PassportDetectionArgs,
    batch_size: int = 16,
    progress: AsyncProgressRateHandler | None = None,
) -> PartialDetectionResult:
    n_pages = await _count_pages(batch)
    if progress is not None:
        progress = to_incremental_async_progress(
            to_raw_async_progress(progress, n_pages)
        )
    errors = []
    im_batches = async_batches(
        _read_images(batch, passport_detector, paths, errors), batch_size
    )
    read_mrz = args.config.inference.passport_detector.read_mrz
    detection_outs = [
        await _detect_passport_pages(
            b, passport_detector, read_mrz=read_mrz, progress=progress
        )
        async for b in im_batches
    ]
    incomplete = {e.file.id for e in errors}
    detection_outs = sum(detection_outs, start=[])
    successes = []
    for res in detection_outs:
        if isinstance(res, FileProcessingError):
            errors.append(res)
            incomplete.add(res.file.id)
        else:
            successes.append(res)
    del detection_outs

    n_success = 0
    n_success_pages = 0
    with_artifacts = set()
    for passport_artifact, n_doc_success_pages in _aggregate_doc_passports(
        successes, incomplete, args
    ):
        if passport_artifact.manifest_entry.status is ManifestEntryStatus.COMPLETE:
            n_success += 1
        with_artifacts.add(passport_artifact.doc_id)
        write_artifact(paths.artifacts, passport_artifact)
        n_success_pages += n_doc_success_pages
    n_errors = len(incomplete)
    if progress is not None:
        await progress(n_errors)
    n_docs = len(with_artifacts.union(incomplete))
    processed = ProcessingReport(n_docs=n_docs, n_pages=n_pages)
    successes = ProcessingReport(n_docs=n_success, n_pages=n_success_pages)
    return PartialDetectionResult(
        processed=processed, successes=successes, errors=errors
    )


async def _count_pages(batch: Path) -> int:
    async with async_open(batch) as f:
        n_pages = 0
        async for line in f:
            if line.strip():
                n_pages += 1
    return n_pages


async def _read_images(
    batch: Path, passport_detector: PassportDetector, paths: WorkerPaths, errors: list
) -> AsyncIterable[tuple[ProcessedFile, "np.ndarray", "DetectionInputs"]]:
    import cv2  # noqa: PLC0415

    for page in read_jsonl_as(paths.workdir / batch, ProcessedPage):
        page_path = page.locate(paths)
        try:
            if not page_path.exists():
                raise FileNotFoundError(f"{page_path} doesn't exist")
            im = cv2.imread(str(page_path))
            if im is None:
                raise InvalidImage(page_path)
        except (InvalidImage, FileNotFoundError) as e:
            logger.error("couldn't read page %s of doc %s!", page_path, page)
            errors.append(FileProcessingError.from_exception(page, e))
            continue
        im, *detection_in = passport_detector.scale_image(im)
        yield page, im, detection_in


async def _detect_passport_pages(
    batch: Iterable[tuple[ProcessedFile, "np.ndarray", "DetectionInputs"]],
    passport_detector: PassportDetector,
    *,
    read_mrz: bool,
    progress: RawAsyncProgressHandler | None = None,
) -> list[tuple[ProcessedFile, list[Passport]] | FileProcessingError]:
    doc_pages, doc_page_ims, detection_ins = zip(*batch, strict=True)
    try:
        passport_pages = await asyncio.to_thread(
            passport_detector.detect_passports, detection_ins
        )
    except InferenceRuntimeError as e:
        doc_pages = list(doc_pages)
        logger.exception("error while running inference on batch: %s", doc_pages)
        return [FileProcessingError.from_exception(d, e) for d in doc_pages]
    if read_mrz:
        passports = [
            [
                await asyncio.to_thread(
                    passport_detector.read_mrz, doc_page_im, passport_page
                )
                for passport_page in doc_page_passport_pages
            ]
            for doc_page_im, doc_page_passport_pages in zip(
                doc_page_ims, passport_pages, strict=True
            )
        ]
    else:
        passports = [
            [
                Passport.from_detection(passport_page)
                for passport_page in doc_page_passport_pages
            ]
            for doc_page_passport_pages in passport_pages
        ]
    if progress is not None:
        await progress(len(passports))
    return list(zip(doc_pages, passports, strict=True))


def _aggregate_doc_passports(
    detection_outs: Iterable[tuple[ProcessedPage, list[Passport]]],
    incomplete: set[str],
    args: PassportDetectionArgs,
) -> Iterable[tuple[PassportArtifact, int]]:
    current_doc = None
    doc_pages_passports = []
    for page, page_passports in detection_outs:
        if current_doc is None:
            current_doc = page
        if current_doc.id != page.id:
            is_complete = current_doc.id not in incomplete
            n_pages = len(doc_pages_passports)
            artifact = _passport_artifact_from_passports(
                current_doc, doc_pages_passports, args, is_complete=is_complete
            )
            yield (artifact, n_pages)
            doc_pages_passports = []
            current_doc = page
        doc_pages_passports.append((page.page_number, page_passports))
    if current_doc:
        is_complete = current_doc.id not in incomplete
        n_pages = len(doc_pages_passports)
        artifact = _passport_artifact_from_passports(
            current_doc, doc_pages_passports, args, is_complete=is_complete
        )
        yield (artifact, n_pages)


def _passport_artifact_from_passports(
    doc: ProcessedPage,
    pages_with_passports: list[tuple[int, list[Passport]]],
    args: PassportDetectionArgs,
    *,
    is_complete: bool,
) -> PassportArtifact:
    pages_with_passports = [
        PagePassports(page_number=p, passports=passports)
        for p, passports in pages_with_passports
        if passports
    ]
    passports = Passports(pages=pages_with_passports)
    manifest_entry = (
        PassportManifestEntry.complete(args)
        if is_complete
        else PassportManifestEntry.partial(args)
    )
    artifact = PassportArtifact(
        project=doc.project,
        doc_id=doc.id,
        artifact=passports.model_dump_json(polymorphic_serialization=True).encode(),
        manifest_entry=manifest_entry,
    )
    return artifact


@cache
def default_country_codes() -> list[str]:
    csv_path = DATA_DIR / "default_country_codes.csv"
    with csv_path.open() as csvfile:
        reader = csv.reader(csvfile)
        countries = [row[2] for row in reader]
    return countries


@PassportDetector.register(PassportDetectorType.YOLO)
class YOLOPassportDetector(PassportDetector):
    def __init__(self, config: YOLOPassportDetectorConfig):
        from passport_service.core.object_detection import (  # noqa:PLC0415
            inference_session,
        )

        self._config = config
        self._path = self._config.model_path
        self._sess_cm = inference_session(self._path)
        self._classes = [self._config.passport_label]
        self._image_size = self._config.image_size
        self._detection_threshold = self._config.detection_threshold
        self._nms_threshold = self._config.nms_threshold
        self._nms_score_threshold = self._config.nms_score_threshold
        self._nms_eta = self._config.nms_eta

    def __enter__(self) -> PassportDetector:
        self._sess = self._sess_cm.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self._sess_cm.__exit__(exc_type, exc_val, exc_tb)

    def detect_passports(
        self, ins: Sequence[tuple["MatLike", float]]
    ) -> list[list[ObjectDetection]]:
        import numpy as np  # noqa: PLC0415
        from onnxruntime.capi.onnxruntime_pybind11_state import Fail  # noqa: PLC0415
        from passport_service.core.object_detection import (  # noqa: PLC0415
            detections_from_nn_output,
        )

        if not ins:
            return []
        blobs, scales = zip(*ins, strict=True)
        blobs = np.concatenate(blobs)
        scales = list(scales)
        input_name = self._sess.get_inputs()[0].name
        label_name = self._sess.get_outputs()[0].name
        model_inputs = {input_name: blobs.astype(np.float32)}
        try:
            outputs = (
                self._sess.run(  # [batch_size, n_classes + dim_box, max_boxes = 8400]
                    [label_name], model_inputs
                )[0]
            )
        except Fail as e:
            msg = "YOLO onnx inference failed"
            raise InferenceRuntimeError(msg) from e
        outputs = np.array(outputs, dtype=np.float32).reshape(
            (-1, outputs.shape[-2], outputs.shape[-1])
        )
        detections = []
        for output, scale in zip(outputs, scales, strict=True):
            detection = detections_from_nn_output(
                output,
                self._classes,
                scale=scale,
                detection_threshold=self._detection_threshold,
                nms_threshold=self._nms_threshold,
                nms_score_threshold=self._nms_score_threshold,
                nms_eta=self._nms_eta,
            )
            detections.append(detection)
        return detections

    def scale_image(self, im: "MatLike") -> "tuple[np.ndarray, MatLike, float]":
        from passport_service.core.object_detection import (  # noqa: PLC0415
            preprocess_image,
        )

        return preprocess_image(im, self._config.image_size)

    def read_mrz(
        self,
        page: "np.array",
        passport: ObjectDetection,
        country_codes: list[str] | None = None,
    ) -> Passport:
        from passport_service.core.object_detection import (  # noqa: PLC0415
            read_passport_mrz,
        )

        if country_codes is None:
            country_codes = default_country_codes()
        mrz = read_passport_mrz(page, passport, country_codes=country_codes)
        passport = Passport.from_detection(passport, mrz)
        return passport

    @classmethod
    def _from_config(cls, config: YOLOPassportDetectorConfig, **extras) -> Self:  # noqa:ARG003
        return cls(config)
