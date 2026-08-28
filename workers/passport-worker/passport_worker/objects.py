import csv
import traceback
from concurrent.futures import ProcessPoolExecutor
from enum import StrEnum
from functools import cache
from pathlib import Path
from typing import Any, ClassVar, Self

from datashare_python.objects import (
    ArtifactType,
    BaseModel,
    DatashareModel,
    DocArtifact,
    DocumentLocation,
    ManifestEntry,
    ProcessedFile,
    ProcessedPage,
    TaskArgs,
    WorkerPaths,
)
from icij_common.pydantic_utils import safe_copy
from icij_common.registrable import RegistrableConfig
from passport_service.constants import (
    DEFAULT_DETECTION_THRESHOLD,
    DEFAULT_NMS_ETA,
    DEFAULT_NMS_SCORE_THRESHOLD,
    DEFAULT_NMS_THRESHOLD,
)
from passport_service.objects import Passport
from pydantic import Field

DocumentSearchQuery = dict[str, Any]
DocId = str
Batches = list[Path]


@cache
def default_country_codes() -> list[str]:
    import passport_service  # noqa: PLC0415

    csv_path = passport_service.DATA_DIR / "default_country_codes.csv"
    with csv_path.open() as csvfile:
        reader = csv.reader(csvfile)
        countries = [row[2] for row in reader]
    return countries


class ImagePreprocessorType(StrEnum):
    DEFAULT = "default"


class ImagePreprocessorConfigBase(DatashareModel, RegistrableConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: ClassVar[ImagePreprocessorType]


class DefaultImagePreprocessorConfig(ImagePreprocessorConfigBase):
    type: ClassVar[ImagePreprocessorType] = Field(default=ImagePreprocessorType.DEFAULT)


# TODO: use a tagged union here when we have more implem
ImagePreprocessorConfig = DefaultImagePreprocessorConfig


class PreprocessingConfig(DatashareModel):
    images: ImagePreprocessorConfig = Field(
        default_factory=DefaultImagePreprocessorConfig
    )


class PassportDetectorType(StrEnum):
    YOLO = "yolo"


class PassportDetectorConfigBase(DatashareModel, RegistrableConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")

    read_mrz: bool = True
    mzr_country_codes: list[str] | None = None


class YOLOPassportDetectorConfig(PassportDetectorConfigBase):
    type: ClassVar[PassportDetectorType] = Field(
        frozen=True, default=PassportDetectorType.YOLO
    )

    model_path: Path
    passport_label: str = "passport"
    detection_threshold: float = DEFAULT_DETECTION_THRESHOLD
    nms_threshold: float = DEFAULT_NMS_THRESHOLD
    nms_score_threshold: float = DEFAULT_NMS_SCORE_THRESHOLD
    nms_eta: float = DEFAULT_NMS_ETA
    image_size: int = 640

    def resolve(self, paths: WorkerPaths) -> "YOLOPassportDetectorConfig":
        update = {"model_path": paths.workdir / self.model_path}
        return safe_copy(self, update=update)


# TODO: use a tagged union here when we have more implem
PassportDetectorConfig = YOLOPassportDetectorConfig


class PassportInferenceConfig(DatashareModel):
    passport_detector: PassportDetectorConfig = Field(
        default_factory=YOLOPassportDetectorConfig
    )


class PassportDetectionConfig(DatashareModel):
    inference: PassportInferenceConfig = Field(default_factory=PassportInferenceConfig)
    preprocessing: PreprocessingConfig = Field(default_factory=PreprocessingConfig)

    def to_image_preprocessing_executor(self) -> ProcessPoolExecutor:
        return self.preprocessing.to_image_preprocessing_executor()


class PassportDetectionArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None
    config: PassportDetectionConfig = Field(default_factory=PassportDetectionConfig)

    def as_manifest_task_input(self) -> dict[str, Any]:
        as_entry = super().as_manifest_task_input()
        as_entry.pop("docs")
        return as_entry


class PreprocessingBatches(BaseModel):
    to_pdf: Batches
    images: Batches
    pdfs: Batches


class PassportManifestEntry(ManifestEntry): ...


class PassportArtifact(DocArtifact):
    filename: ClassVar[str] = "passports.json"
    type: ClassVar[ArtifactType] = ArtifactType.PASSPORTS


class ProcessingReport(DatashareModel):
    n_docs: int = 0
    n_pages: int = 0

    def __add__(self, other: Self) -> Self:
        return ProcessingReport(
            n_docs=other.n_docs + self.n_docs, n_pages=other.n_pages + self.n_pages
        )


class PagePassports(DatashareModel):
    page_number: int
    passports: list[Passport]


class Passports(DatashareModel):
    pages: list[PagePassports] = []


class Error(BaseModel):
    title: str
    detail: str

    @classmethod
    def from_exception(cls, exception: BaseException) -> Self:
        title = exception.__class__.__name__
        trace_lines = traceback.format_exception(
            None, value=exception, tb=exception.__traceback__
        )
        detail = f"{exception}\n{''.join(trace_lines)}"
        error = Error(title=title, detail=detail)
        return error


class FileProcessingError(BaseModel):
    file: ProcessedFile
    error: Error

    @classmethod
    def from_exception(cls, file: ProcessedFile, exception: BaseException) -> Self:
        return cls(file=file, error=Error.from_exception(exception))


class ProcessingError(DatashareModel):
    location: DocumentLocation
    path: Path
    page: int | None = None
    error: Error

    @classmethod
    def from_file_processing_error(cls, fp_error: FileProcessingError) -> Self:
        page = None
        if isinstance(fp_error.file, ProcessedPage):
            page = fp_error.file.page_number
        return cls(
            location=fp_error.file.location,
            path=fp_error.file.path,
            page=page,
            error=fp_error.error,
        )


# TODO: should it be in datashare-python ?
class DocumentErrors(DatashareModel):
    doc_id: str
    project: str
    location: DocumentLocation
    path: Path
    errors: list[ProcessingError]


class ErrorReport(ProcessingReport):
    errors: list[DocumentErrors] = []

    @classmethod
    def from_exception(cls, file: ProcessedFile, exception: BaseException) -> Self:
        return cls.from_file_processing_errors(
            FileProcessingError.from_exception(file, exception)
        )

    @classmethod
    def from_file_processing_errors(
        cls,
        *file_processing_errors: FileProcessingError,
    ) -> Self:
        roots = dict()
        n_pages = 0
        for error in file_processing_errors:
            root = error.file
            while (parent := root.parent) is not None:
                root = parent
            root_errors = roots.get(root.id)
            if root_errors is not None:
                _, root_errors = root_errors
            else:
                root_errors = []
            if isinstance(error.file, ProcessedPage):
                n_pages += 1
            root_errors.append(ProcessingError.from_file_processing_error(error))
            roots[root.id] = (root, root_errors)
        errors = []
        for _, (root, root_errors) in sorted(roots.items()):
            doc_errors = DocumentErrors(
                doc_id=root.id,
                project=root.project,
                location=root.location,
                path=root.path,
                errors=root_errors,
            )
            errors.append(doc_errors)
        return cls(n_docs=len(errors), n_pages=n_pages, errors=errors)


class PartialDetectionResult(BaseModel):
    processed: ProcessingReport = Field(default_factory=ProcessingReport)
    successes: ProcessingReport = Field(default_factory=ProcessingReport)
    errors: list[FileProcessingError]


class PassportDetectionResponse(DatashareModel):
    processed: ProcessingReport = Field(default_factory=ProcessingReport)
    successes: ProcessingReport = Field(default_factory=ProcessingReport)
    errors: ErrorReport = Field(default_factory=ErrorReport)

    @classmethod
    def aggregate(
        cls,
        preprocessing_errors: list[FileProcessingError],
        *,
        inference_results: list[PartialDetectionResult],
    ) -> Self:
        errors = preprocessing_errors + sum((r.errors for r in inference_results), [])
        errors = ErrorReport.from_file_processing_errors(*errors)
        processed = sum(
            (r.processed for r in inference_results), start=ProcessingReport()
        )
        successes = sum(
            (r.successes for r in inference_results), start=ProcessingReport()
        )
        return cls(processed=processed, successes=successes, errors=errors)
