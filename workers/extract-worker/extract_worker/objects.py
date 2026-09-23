from typing import Any, ClassVar

from datashare_python.objects import (
    ArtifactType,
    DatashareModel,
    DocArtifact,
    ErrorReportWithPages,
    ManifestEntry,
    Pages,
    ProcessedFile,
    ProcessingError,
    ProcessingReportWithPages,
    TaskArgs,
)
from extract_core import DoclingPipelineConfig, PipelineConfig, PipelineType, Status

# Import the config class from extract python otherwise the
# ExtractPipelineConfig.__subclasses__ list might be incomplete
from icij_common.pydantic_utils import make_enum_discriminator
from pydantic import Field

DocumentSearchQuery = dict[str, Any]
DocId = str


pipeline_discriminator = make_enum_discriminator("pipeline", PipelineType)


class MarkdownExtractArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None
    config: PipelineConfig = Field(default_factory=DoclingPipelineConfig)

    def as_manifest_task_input(self) -> dict[str, Any]:
        as_entry = super().as_manifest_task_input()
        as_entry.pop("docs")
        return as_entry


class StructureArtifact(DocArtifact):
    filename: ClassVar[str] = "structure"
    type: ClassVar[ArtifactType] = ArtifactType.STRUCTURE


class StructureManifestEntry(ManifestEntry):
    confidence: float | None = None
    pages: Pages


class ExtractError(ProcessingError[ProcessedFile]):
    status: Status


class MarkdownExtractResponse(DatashareModel):
    processed: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    successes: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    errors: ErrorReportWithPages = Field(default_factory=ErrorReportWithPages)

    def __add__(self, other: "MarkdownExtractResponse") -> "MarkdownExtractResponse":
        return MarkdownExtractResponse(
            processed=self.processed + other.processed,
            successes=self.successes + other.successes,
            errors=self.errors + self.errors,
        )
