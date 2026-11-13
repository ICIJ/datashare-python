from collections.abc import Sequence
from pathlib import Path
from typing import Any

from datashare_python.objects import (
    DatashareModel,
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingError,
    ProcessingReportWithPages,
    StructureArtifact,  # noqa: F401
    StructureManifestEntry,  # noqa: F401
    TaskArgs,
)
from extract_core import PipelineBySize, PipelineConfig, Status

# Import the config class from extract python otherwise the
# ExtractPipelineConfig.__subclasses__ list might be incomplete
from pydantic import Field, model_validator

from extract_worker.constants import (
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_MODEL_REVISION,
)

from .constants import CHUNK_DOC_ID, CHUNK_INDEX

DocumentSearchQuery = dict[str, Any]
DocId = str
Vector = Sequence[float | int]


class MarkdownExtractArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None
    config: PipelineConfig = Field(default_factory=PipelineBySize)

    def as_manifest_task_input(self) -> dict[str, Any]:
        as_entry = super().as_manifest_task_input()
        as_entry.pop("docs")
        return as_entry


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
            errors=self.errors + other.errors,
        )


class EmbeddingArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None
    model: str = DEFAULT_EMBEDDING_MODEL
    # pin the model revision to avoid silently pulling new remote code
    model_revision: str | None = DEFAULT_EMBEDDING_MODEL_REVISION
    # embeddings are truncated to this size, it must match the index vector size
    dim: int = DEFAULT_EMBEDDING_DIM

    def as_manifest_task_input(self) -> dict[str, Any]:
        as_entry = super().as_manifest_task_input()
        as_entry.pop("docs")
        return as_entry


class EmbeddedChunk(DatashareModel):
    doc_id: str
    root_document: str
    chunk_index: int
    page: int
    text: str
    headings: list[str] | None = None
    model: str
    embedding: list[float]
    id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _add_es_id(cls, v: Any) -> Any:
        if isinstance(v, dict):
            doc_id = v.get(CHUNK_DOC_ID, v["doc_id"])
            chunk_index = v.get(CHUNK_INDEX, v["chunk_index"])
            v["id"] = f"{doc_id}-{chunk_index}"
        else:
            doc_id = v.doc_id
            chunk_index = v.chunk_index
            v.id = f"{doc_id}-{chunk_index}"
        return v


class EmbeddingError(ProcessingError[ProcessedFile]): ...


class EmbeddingResponse(DatashareModel):
    processed: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    successes: ProcessingReportWithPages = Field(
        default_factory=ProcessingReportWithPages
    )
    errors: ErrorReportWithPages = Field(default_factory=ErrorReportWithPages)
    n_chunks: int = 0

    def __add__(self, other: "EmbeddingResponse") -> "EmbeddingResponse":
        return EmbeddingResponse(
            processed=self.processed + other.processed,
            successes=self.successes + other.successes,
            errors=self.errors + other.errors,
            n_chunks=self.n_chunks + other.n_chunks,
        )


class EmbedBatchResult(DatashareModel):
    chunks: Path
    n_chunks_by_doc: dict[DocId, int]
    response: EmbeddingResponse
