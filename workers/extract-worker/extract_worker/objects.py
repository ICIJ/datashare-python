from typing import Any, ClassVar, Self

from datashare_python.objects import (
    ArtifactType,
    ByteRangesPagination,
    DatashareModel,
    DocArtifact,
    FilesystemDocument,
    ManifestEntry,
    TaskArgs,
)
from extract_core import (
    DoclingPipelineConfig,
    Error,
    PipelineConfig,
    PipelineType,
    Status,
)

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
    pages: ByteRangesPagination


class ProcessingReport(DatashareModel):
    n_docs: int = 0
    n_pages: int = 0

    def __add__(self, other: Self) -> Self:
        return ProcessingReport(
            n_docs=other.n_docs + self.n_docs, n_pages=other.n_pages + self.n_pages
        )


class ProcessedDoc(FilesystemDocument):
    n_pages: int

    @classmethod
    def from_fs_doc(cls, fs_doc: FilesystemDocument, n_pages: int | None) -> Self:
        n_pages = n_pages if n_pages is not None else 1
        return cls(n_pages=n_pages, **fs_doc.model_dump())


class ErrorReport(DatashareModel):
    doc: ProcessedDoc
    status: Status
    errors: list[Error] = []


class MarkdownExtractResponse(DatashareModel):
    processed: ProcessingReport = Field(default_factory=ProcessingReport)
    successes: ProcessingReport = Field(default_factory=ProcessingReport)
    errors: list[ErrorReport] = Field(default_factory=list)

    @classmethod
    def from_responses(cls, *responses: Self) -> Self:
        processed = sum((r.processed for r in responses), start=ProcessingReport())
        successes = sum((r.successes for r in responses), start=ProcessingReport())
        errors = sum((r.errors for r in responses), start=[])
        return cls(processed=processed, successes=successes, errors=errors)
