import csv
import operator
from concurrent.futures import ProcessPoolExecutor
from enum import StrEnum
from functools import cache, reduce
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from datashare_python.objects import (
    ArtifactType,
    BaseModel,
    DatashareModel,
    DocArtifact,
    ErrorReport,
    ManifestEntry,
    ProcessedFile,
    ProcessingError,
    ProcessingReport,
    ProcessingReportWithPages,
    TaskArgs,
    WorkerFile,
    WorkerPaths,
)
from icij_common.pydantic_utils import make_enum_discriminator, safe_copy, tagged_union
from icij_common.registrable import RegistrableConfig
from passport_service.constants import (
    DEFAULT_DETECTION_THRESHOLD,
    DEFAULT_NMS_ETA,
    DEFAULT_NMS_SCORE_THRESHOLD,
    DEFAULT_NMS_THRESHOLD,
)
from passport_service.objects import Passport
from pydantic import Discriminator, Field

DocumentSearchQuery = dict[str, Any]
DocId = str
Batches = list[Path]
OnnxProvider = Literal["CUDAExecutionProvider", "CPUExecutionProvider"]


class ExportFormat(StrEnum):
    CSV = "csv"
    JSONL = "jsonl"


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


class PDFPreprocessorType(StrEnum):
    DEFAULT = "default"
    LITEPARSE = "liteparse"


class PDFPreprocessorConfigBase(DatashareModel, RegistrableConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: ClassVar[PDFPreprocessorType]


class DefaultPDFPreprocessorConfig(PDFPreprocessorConfigBase):
    type: ClassVar[PDFPreprocessorType] = Field(
        frozen=True, default=PDFPreprocessorType.DEFAULT
    )


class OCRReason(StrEnum):
    SCANNED = "scanned"
    NO_TEXT = "no-text"
    SPARSE_TEXT = "sparse-text"
    EMBEDDED_IMAGES = "embedded-images"
    GARBLED = "garbled"
    VECTOR_TEXT = "vector-text"
    ANNOTATION_TEXT = "annotation-text"


class LayoutReason(StrEnum):
    MULTI_COLUMN = "multi-column"
    TABLE_LIKELY = "table-likely"
    DENSE_GRAPHICS = "dense-graphics"


def _all_ocr_reasons() -> set[OCRReason]:
    return set(OCRReason)


class LiteparsePageFilter(BaseModel):
    accept_ocr_reasons: set[OCRReason] = Field(default_factory=_all_ocr_reasons)
    accept_layout_reasons: set[LayoutReason] = Field(default_factory=set)


class LiteparsePDFPreprocessorConfig(PDFPreprocessorConfigBase):
    type: ClassVar[PDFPreprocessorType] = Field(
        frozen=True, default=PDFPreprocessorType.LITEPARSE
    )
    preprocessor: DefaultPDFPreprocessorConfig = Field(
        default_factory=DefaultPDFPreprocessorConfig
    )
    page_filter: LiteparsePageFilter = Field(default_factory=LiteparsePageFilter)


PDFPreprocessorConfig = tagged_union(
    PDFPreprocessorConfigBase.__subclasses__(), lambda t: t.type.default
)
pdf_preprocessor_discriminator = make_enum_discriminator("type", PDFPreprocessorType)


class PreprocessingConfig(DatashareModel):
    use_caching: bool = True

    images: ImagePreprocessorConfig = Field(
        default_factory=DefaultImagePreprocessorConfig
    )
    pdfs: PDFPreprocessorConfig = Field(
        discriminator=Discriminator(pdf_preprocessor_discriminator),
        default_factory=DefaultPDFPreprocessorConfig,
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

    onnx_provider: OnnxProvider = "CUDAExecutionProvider"
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


class PassportExportArgs(TaskArgs):
    project: str
    output_path: Path
    format: ExportFormat = ExportFormat.CSV
    docs: list[DocId] | DocumentSearchQuery | None = None


class PreprocessingBatches(BaseModel):
    to_pdf: Batches
    images: Batches
    pdfs: Batches


@ManifestEntry.register(ArtifactType.PASSPORTS)
class PassportManifestEntry(ManifestEntry): ...


class PassportArtifact(DocArtifact):
    filename: ClassVar[str] = "passports.json"
    type: ClassVar[ArtifactType] = ArtifactType.PASSPORTS


class ProcessedPage(WorkerFile):
    page: int

    @classmethod
    def from_parent(
        cls,
        parent: ProcessedFile,
        path: Path,
        paths: WorkerPaths,
        page: int,
    ) -> "ProcessedPage":
        return cls(
            page=page, **WorkerFile.from_parent(parent, path, paths).model_dump()
        )

    @property
    def n_pages(self) -> int:
        return 1


class PassportProcessingError(ProcessingError[ProcessedPage | ProcessedFile]): ...


class PagePassports(DatashareModel):
    page_number: int
    passports: list[Passport]


class Passports(DatashareModel):
    pages: list[PagePassports] = Field(default_factory=list)


class PartialDetectionResult(BaseModel):
    processed: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    successes: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    errors: list[PassportProcessingError]


class PassportDetectionResponse(DatashareModel):
    processed: ProcessingReport = Field(default_factory=ProcessingReport)
    successes: ProcessingReport = Field(default_factory=ProcessingReport)
    errors: ErrorReport = Field(default_factory=ErrorReport)

    @classmethod
    def aggregate(
        cls,
        errors: list[PassportProcessingError],
        *,
        inference_results: list[PartialDetectionResult],
    ) -> Self:
        errors += reduce(operator.iadd, (r.errors for r in inference_results), [])
        errors = ErrorReport.from_errors(*errors)
        processed = sum(
            (r.processed for r in inference_results), start=ProcessingReport()
        )
        successes = sum(
            (r.successes for r in inference_results), start=ProcessingReport()
        )
        return cls(processed=processed, successes=successes, errors=errors)


class PassportExportResponse(DatashareModel):
    export_path: Path
