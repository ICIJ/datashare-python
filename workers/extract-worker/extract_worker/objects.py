from typing import Annotated, Any, Self

from datashare_python.objects import DatashareModel, FilesystemDocument
from extract_python import (
    DoclingPipelineConfig,
    PipelineType,
    Status,
)
from extract_python import (
    PipelineConfig as ExtractPipelineConfig,
)
from extract_python.objects import Error
from icij_common.pydantic_utils import make_enum_discriminator, tagged_union
from pydantic import Discriminator, Field

DocumentSearchQuery = dict[str, Any]
DocId = str


pipeline_discriminator = make_enum_discriminator("pipeline", PipelineType)
PipelineConfig = Annotated[
    tagged_union(ExtractPipelineConfig.__subclasses__(), lambda t: t.pipeline.default),
    Discriminator(pipeline_discriminator),
]


class MarkdownExtractArgs(DatashareModel):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None
    config: PipelineConfig = Field(default_factory=DoclingPipelineConfig)


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
