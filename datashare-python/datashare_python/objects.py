import hashlib
import json
import logging
import os
import traceback
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, unique
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Self, TypeVar, cast, final

import langcodes
from icij_common.registrable import Registrable
from pydantic_core import PydanticCustomError, ValidationError, core_schema
from pydantic_core.core_schema import PlainValidatorFunctionSchema
from pydantic_extra_types.language_code import LanguageName
from temporalio import activity, workflow

from .constants import TIKA_METADATA_RESOURCENAME

with workflow.unsafe.imports_passed_through():
    from icij_common.es import (
        DOC_CONTENT,
        DOC_CONTENT_TRANSLATED,
        DOC_EXTRACTION_LEVEL,
        DOC_LANGUAGE,
        DOC_METADATA,
        DOC_PATH,
        DOC_ROOT_ID,
        ID_,
        INDEX_,
        SOURCE,
    )

from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    make_enum_discriminator,
    merge_configs,
    no_enum_values_config,
    tagged_union,
)
from pydantic import (
    AfterValidator,
    AliasChoices,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    GetCoreSchemaHandler,
    Tag,
    TypeAdapter,
    model_validator,
)
from pydantic import BaseModel as _BaseModel
from pydantic.main import IncEx

logger = logging.getLogger(__name__)


Routing = str
DocID = str
DocRoute = tuple[DocID, Routing]


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())

    def __hash__(self) -> int:
        digest = hashlib.md5(
            json.dumps(self.model_dump(mode="json"), sort_keys=True).encode()
        ).digest()
        return int.from_bytes(digest[:8])


class DatashareModel(BaseModel):
    model_config = merge_configs(BaseModel.model_config, lowercamel_case_config())


class DatashareLanguage(str):
    _language_type_adapter: ClassVar[TypeAdapter] = TypeAdapter(LanguageName)

    @classmethod
    def _validate(cls, __input_value: str, _: core_schema.ValidationInfo) -> Self:
        if __input_value != __input_value.upper():
            raise PydanticCustomError(
                "datashare_language", "Invalid Datashare language, expected uppercase"
            )
        try:
            # Use pydantic provided validation
            cls._language_type_adapter.validate_python(__input_value.title())
        except ValidationError as e:
            raise PydanticCustomError(
                "datashare_language", "Unknown Datashare language"
            ) from e
        return cls(__input_value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.AfterValidatorFunctionSchema:
        return core_schema.with_info_after_validator_function(
            cls._validate,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

    @property
    def as_language_name(self) -> LanguageName:
        return LanguageName(self.title())

    @property
    def alpha2(self) -> str | None:
        return self.as_language_name.alpha2

    @property
    def alpha3(self) -> str:
        return self.as_language_name.alpha3


@unique
class FileLocation(StrEnum):
    FILESYSTEM = "filesystem"
    ARTIFACTS = "artifacts"
    WORKDIR = "workdir"


class WorkerPaths(BaseModel):
    filesystem: Path
    artifacts: Path
    workdir: Path

    def locate(self, path: Path, location: FileLocation) -> Path:
        match location:
            case FileLocation.FILESYSTEM:
                return self.filesystem / path
            case FileLocation.ARTIFACTS:
                return self.artifacts / path
            case FileLocation.WORKDIR:
                return self.workdir / path
            case _:
                raise ValueError(f"invalid location: {path}")


class FromParent[P](ABC):
    @classmethod
    @abstractmethod
    def from_parent(cls, parent: P, *args, **kwargs) -> Self: ...


class IETFLanguage(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> PlainValidatorFunctionSchema:
        return core_schema.no_info_plain_validator_function(cls.validate)

    @classmethod
    def validate(cls, v: Any) -> Self:
        tag = langcodes.get(str(v))
        if not tag.is_valid():
            raise ValueError(f"Invalid IETF language: {v}")
        return cls(v)


Language = DatashareLanguage | IETFLanguage


def _from_sentences(value: Any) -> Any:
    if isinstance(value, list):
        return " ".join(value)
    return value


class Translation(BaseModel):  # No camelcase here we don't know why
    source_language: DatashareLanguage
    target_language: Language
    translator: str
    content: Annotated[str, BeforeValidator(_from_sentences)]


class Document(DatashareModel):
    """ES datashare document"""

    id: str
    language: DatashareLanguage
    index: str | None = None
    root_document: str | None = None
    content: Annotated[str, BeforeValidator(_from_sentences)]
    extraction_level: int = 0
    content: str | None = None
    content_text_length: int | None = None
    content_type: str | None = None
    path: Path | None = None
    tags: list[str] = Field(default_factory=list)
    content_translated: list[Translation] | None = Field(
        default=None,
        # es translator is using snake_case, we must do the same
        alias="content_translated",
    )
    metadata: dict[str, Any] | None = None
    type: str = Field(default="Document", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def _initialize_content_length_from_content(cls, data: Any) -> Any:
        if isinstance(data, dict):
            content_length = data.get("content_text_length")
            if content_length is None and (content := data.get(DOC_CONTENT)):
                data["content_text_length"] = len(content)
        return data

    @model_validator(mode="before")
    @classmethod
    def _set_root_document_to_self_when_not_provided(cls, data: Any) -> Any:
        if isinstance(data, dict):
            root = data.get("root_document")
            if root is None:
                data["root_document"] = data.get("id")
        return data

    @model_validator(mode="after")
    def _validate_root_document(self) -> Any:
        if self.root_document is None:
            msg = (
                "inconsistent state, root_document cannot be after validation, "
                "there's a bug in this object's implementation."
                " Please report to a developer."
            )
            raise ValueError(msg)
        if self.is_root_document:
            if self.extraction_level > 0:
                msg = "extraction_level should be <= 0 for root documents"
                raise ValueError(msg)
        elif self.extraction_level <= 0:
            msg = "extraction_level should be > 0 for embedded documents"
            raise ValueError(msg)
        return self

    @property
    def is_root_document(self) -> bool:
        return self.root_document == self.id and self.extraction_level <= 0

    @classmethod
    def from_es(cls, es_doc: dict) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            id=es_doc[ID_],
            index=es_doc.get(INDEX_),
            content=sources.get(DOC_CONTENT),
            content_translated=sources.get(DOC_CONTENT_TRANSLATED),
            content_text_length=sources.get("content_text_length"),
            language=DatashareLanguage(sources[DOC_LANGUAGE]),
            root_document=sources.get(DOC_ROOT_ID),
            extraction_level=sources.get(DOC_EXTRACTION_LEVEL),
            tags=sources.get("tags", []),
            path=sources.get(DOC_PATH),
            metadata=sources.get(DOC_METADATA),
        )

    @property
    def project(self) -> str:
        if self.index is None:
            raise ValueError("missing index")
        return self.index

    @property
    def doc_id(self) -> str:
        return self.id

    @property
    def route(self) -> DocRoute:
        if self.root_document:
            return self.id, self.id
        return self.id, self.root_document

    @property
    def resource_name(self) -> str:
        if self.metadata is None:
            raise ValueError("missing metadata")
        resource_name = self.metadata.get(TIKA_METADATA_RESOURCENAME)
        if resource_name is None:
            msg = f"missing {TIKA_METADATA_RESOURCENAME} in metadata"
            raise KeyError(msg)
        cast(str, resource_name)
        return resource_name


def _is_relative(value: Path) -> Path:
    if value.is_absolute():
        raise ValueError(
            f"WorkerFilePath path should always be relative, found {value}"
        )
    return value


class WorkerFilePath(BaseModel):
    """Path of a file produced or processed by a worker, located somewhere on the DS
    filesystem, artifactsdir or workdir
    """

    path: Annotated[Path, AfterValidator(_is_relative)]
    location: FileLocation

    def locate(self, paths: WorkerPaths) -> Path:
        return paths.locate(self.path, self.location)

    @classmethod
    def relative_to_workdir(cls, path: Path, paths: WorkerPaths) -> Self:
        path = path.relative_to(paths.workdir)
        return cls(path=path, location=FileLocation.WORKDIR)


def _is_absolute_path(v: bytes | BytesIO | Path) -> Any:
    if isinstance(v, Path) and not v.is_absolute():
        raise ValueError("artifact path must be absolute")
    return v


class ArtifactType(StrEnum):
    STRUCTURE = "structure"
    ASR_TRANSCRIPTION = "transcription"
    PASSPORTS = "passports"


class ManifestEntryStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"


class User(DatashareModel):
    id: str
    name: str | None = None
    email: str | None = None
    provider: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class TaskArgs(DatashareModel, ABC):
    user: User | None = None

    def as_manifest_task_input(self) -> dict[str, Any]:
        # This is a base implementation, if the input is too large to be dumped,
        # override this and pop large keys
        # Dump in json mode to make testing easier
        as_manifest = self.model_dump(by_alias=True, mode="json")
        return as_manifest


A = TypeVar("A", bound=TaskArgs)


class ManifestEntry[A](DatashareModel, ABC):
    status: ManifestEntryStatus
    # TODO: make this one non optional in the next major !
    task_id: str | None
    label: str | None = None
    input: Annotated[
        dict[str, Any] | None,
        Field(
            validation_alias=AliasChoices("taskInput", "input"),
            serialization_alias="taskInput",
        ),
    ]

    @classmethod
    def complete(cls, args: A, label: str | None = None, **kwargs) -> Self:
        task_id = None
        if activity.in_activity():
            task_id = activity.info().workflow_id
        return cls(
            task_id=task_id,
            input=args.as_manifest_task_input(),
            label=label,
            status=ManifestEntryStatus.COMPLETE,
            **kwargs,
        )

    @classmethod
    def partial(cls, args: A, label: str | None = None, **kwargs) -> Self:
        task_id = None
        if activity.in_activity():
            task_id = activity.info().workflow_id
        return cls(
            task_id=task_id,
            input=args.as_manifest_task_input(),
            label=label,
            status=ManifestEntryStatus.PARTIAL,
            **kwargs,
        )


class PaginationType(StrEnum):
    FILESYSTEM = "filesystem"
    BYTE_RANGES = "byteRanges"


def _validate_pages_range(v: Any) -> None:
    if not isinstance(v, list):
        msg = f"expected a list, got {type(v)}"
        raise TypeError(msg)
    previous_end = None
    for page_i, (start, end) in enumerate(v):
        if not start <= end:
            msg = "end of page must be >= start"
            raise ValueError(msg)
        if previous_end is not None and previous_end != start:
            msg = (
                f"start of page {page_i} doesn't match end of previous "
                f"page {previous_end}"
            )
            raise ValueError(msg)
    return v


PagesRange = Annotated[list[tuple[int, int]], AfterValidator(_validate_pages_range)]


class BasePagination(Registrable, DatashareModel):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: ClassVar[PaginationType] = Field(frozen=True)


@BasePagination.register(PaginationType.FILESYSTEM)
class FilesystemPagination(BasePagination):
    type: ClassVar[PaginationType] = Field(
        default=PaginationType.FILESYSTEM, frozen=True
    )


@BasePagination.register(PaginationType.BYTE_RANGES)
class ByteRangesPagination(BasePagination):
    type: ClassVar[PaginationType] = Field(
        default=PaginationType.BYTE_RANGES, frozen=True
    )
    byte_ranges: PagesRange


pagination_discriminator = make_enum_discriminator("type", PaginationType)
Pagination = Annotated[
    tagged_union(BasePagination.__subclasses__(), lambda x: x.type.default),
    Discriminator(pagination_discriminator),
]


class Pages(DatashareModel):
    total: int
    pagination: Pagination

    @model_validator(mode="after")
    def byte_ranges_length_should_match_total(self) -> Self:
        if (
            isinstance(self.pagination, ByteRangesPagination)
            and len(self.pagination.byte_ranges) != self.total
        ):
            n_pages = len(self.pagination.byte_ranges)
            msg = (
                f"byte_ranges must match total. Found {n_pages} for"
                f" byte_ranges and  {self.total} for total."
            )
            raise ValueError(msg)
        return self


class DocArtifact(DatashareModel, ABC):
    # This object is not used for serde, just as a container, it's OK to allow
    # arbitrary types (to allow storing BytesIO)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    project: str
    doc_id: str
    artifact: Annotated[bytes | BytesIO | Path, AfterValidator(_is_absolute_path)]
    filename: ClassVar[str]  # Override this
    type: ClassVar[ArtifactType]  # Override this
    manifest_entry: ManifestEntry


@unique
class TaskState(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"


READY_STATES = frozenset({TaskState.DONE, TaskState.ERROR, TaskState.CANCELLED})


class StacktraceItem(DatashareModel):
    name: str
    file: str
    lineno: int


class Message(DatashareModel):
    type: str = Field(frozen=True, alias="@type")

    def model_dump(
        self,
        *,
        mode: Literal["json", "python"] | str = "python",
        include: IncEx | None = None,
        exclude: IncEx | None = None,
        context: Any | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal["none", "warn", "error"] = True,
        fallback: Callable[[Any], Any] | None = None,
        serialize_as_any: bool = False,
    ) -> dict[str, Any]:
        return super().model_dump(
            by_alias=True,
            mode=mode,
            include=include,
            exclude=exclude,
            context=context,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
            fallback=fallback,
            serialize_as_any=serialize_as_any,
        )


class TaskResult(Message):
    type: str = Field(frozen=True, alias="@type", default="TaskResult")
    value: object


class TaskError(Message):
    type: str = Field(frozen=True, alias="@type", default="TaskError")
    name: str
    message: str
    cause: str | None = None
    stacktrace: list[StacktraceItem] = Field(default_factory=list)


def _datetime_now() -> datetime:
    return datetime.now(UTC)


class User(Message):
    type: str = Field(
        frozen=True, alias="@type", default="org.icij.datashare.user.User"
    )
    id: str
    name: str | None = None
    email: str | None = None
    provider: str | None = None
    details: dict = dict()


class Task(Message):
    type: str = Field(frozen=True, alias="@type", default="Task")
    id: str
    name: str
    args: dict[str, object] | None = None
    state: TaskState = TaskState.CREATED
    result: TaskResult | None = None
    error: TaskError | None = None
    progress: float | None = None
    created_at: datetime = Field(default_factory=_datetime_now)
    completed_at: datetime | None = None
    retries_left: int | None = None
    max_retries: int | None = None


@dataclass(frozen=True)
class TaskGroup:
    name: str

    @property
    @classmethod
    def python(cls) -> Self:
        return cls(name="PYTHON")


class Error(BaseModel):
    title: str
    detail: str | None

    @classmethod
    def from_exception(cls, exception: BaseException) -> "Error":
        title = exception.__class__.__name__
        trace_lines = traceback.format_exception(
            None, value=exception, tb=exception.__traceback__
        )
        detail = f"{exception}\n{''.join(trace_lines)}"
        error = Error(title=title, detail=detail)
        return error

    def without_detail(self) -> "Error":
        return Error(title=self.title, detail=None)


class ErrorSource(BaseModel, ABC):
    """Source of the processing error."""

    doc_id: str
    project: str


class ErrorSourceWithPages(ErrorSource, ABC):
    """Source of the processing error and number of pages processed at that time"""

    n_pages: int


class ProcessingError[S: ErrorSource](BaseModel):
    """Error occurring while processing a document or intermediate result"""

    source: S
    error: Error

    @final
    @classmethod
    def from_exception(cls, source: S, exception: BaseException) -> Self:
        return cls(source=source, error=Error.from_exception(exception))


class ProcessingReport(DatashareModel):
    """Basic processing report indicating how many docs where processed"""

    n_docs: int = 0

    def __add__(self, other: Self) -> Self:
        return ProcessingReport(n_docs=other.n_docs + self.n_docs)


class ProcessingReportWithPages(ProcessingReport):
    """Processing report indicating how many pages where processed"""

    n_pages: int = 0

    def __add__(self, other: Self) -> Self:
        return ProcessingReportWithPages(
            n_docs=other.n_docs + self.n_docs, n_pages=other.n_pages + self.n_pages
        )


def _without_detail(errors: list[Error]) -> list[Error]:
    if any(e.detail is not None for e in errors):
        msg = "expected errors without details"
        raise ValueError(msg)
    return errors


class DocProcessingErrors(BaseModel):
    """All errors related to several processing happening on a single document.
    If a source doc has many children (typically a document split into pages
    processed separately), all children errors are merged under the older parent.
    """

    doc_id: str
    root_document: str
    project: str
    errors: Annotated[
        list[Error], AfterValidator(_without_detail), Field(default_factory=list)
    ]


class ErrorReport(ProcessingReport):
    """Processing report indicated how many docs failed and listing errors per doc."""

    errors: list[DocProcessingErrors] = Field(default_factory=list)

    def __add__(self, other: Self) -> Self:
        return ErrorReportWithPages(
            n_docs=other.n_docs + self.n_docs, errors=self.errors + other.errors
        )

    @classmethod
    def from_errors(cls, *errors: ProcessingError) -> Self:
        """Aggregate errors by root source documents"""
        roots = dict()
        for error in errors:
            root = error.source
            while (parent := getattr(root, "parent", None)) is not None:
                root = parent
            root_errors = roots.get(root.doc_id)
            if root_errors is not None:
                _, root_errors = root_errors
            else:
                root_errors = []
            root_errors.append(error)
            roots[root.doc_id] = (root, root_errors)
        errors = []
        for _, (root, root_errors) in sorted(roots.items()):
            # We keep details for errors stored as intermediate steps to allow
            # debugging, we don't output them in the response to keep everything
            # lightweight and more secure
            root_errors = [err.error.without_detail() for err in root_errors]  # noqa: PLW2901
            doc_errors = DocProcessingErrors(
                doc_id=root.doc_id,
                project=root.project,
                root_document=root.root_document,
                errors=root_errors,
            )
            errors.append(doc_errors)
        return cls(n_docs=len(errors), errors=errors)


class ErrorReportWithPages(ProcessingReportWithPages, ABC):
    """Same as ErrorReport but providing details about how many pages failed"""

    errors: list[DocProcessingErrors] = Field(default_factory=list)

    def __add__(self, other: Self) -> Self:
        return ErrorReportWithPages(
            n_docs=other.n_docs + self.n_docs,
            n_pages=other.n_pages + self.n_pages,
            errors=self.errors + other.errors,
        )

    @classmethod
    def from_errors(cls, *errors: ProcessingError) -> Self:
        """Aggregate errors by root source documents"""
        roots = dict()
        n_pages = 0
        for error in errors:
            root = error.source
            while (parent := getattr(root, "parent", None)) is not None:
                root = parent
            root_errors = roots.get(root.doc_id)
            if root_errors is not None:
                _, root_errors = root_errors
            else:
                root_errors = []
            n_pages += error.source.n_pages
            root_errors.append(error)
            roots[root.doc_id] = (root, root_errors)
        errors = []
        for _, (root, root_errors) in sorted(roots.items()):
            # We keep details for errors stored as intermediate steps to allow
            # debugging, we don't output them in the response to keep everything
            # lightweight and more secure
            root_errors = [err.error.without_detail() for err in root_errors]  # noqa: PLW2901
            doc_errors = DocProcessingErrors(
                doc_id=root.doc_id,
                project=root.project,
                root_document=root.root_document,
                errors=root_errors,
            )
            errors.append(doc_errors)
        return cls(n_docs=len(errors), n_pages=n_pages, errors=errors)


class ProcessingResult[V](BaseModel):
    value: V


class WithParent[P](FromParent[P]):
    """Result produced from another processing result

    Tracking parent allows track errors from one intermediate results to another back
     to the initial DS Document from the index. It also allows error aggregation.
    """

    parent: P

    @property
    def doc_id(self) -> str:
        return self.parent.doc_id

    @property
    def root_document(self) -> str:
        return self.parent.root_document

    @property
    def project(self) -> str:
        return self.parent.project

    @property
    def route(self) -> DocRoute:
        return self.parent.route


class DatashareFile(
    WithParent[Document], ProcessingResult[WorkerFilePath], ErrorSourceWithPages
):
    """A datashare document file ready to be processed and located somewhere on the
    filesystem or artifact directory.
    """

    parent: Document

    def locate(self, paths: WorkerPaths) -> Path:
        return self.value.locate(paths)

    @classmethod
    def from_parent(cls, parent: Document) -> Self:
        from .utils import artifacts_dir  # noqa: PLC0415

        if parent.metadata is None:
            raise ValueError(
                "can't compute filesystem path for document withtout metadata"
            )
        if parent.is_root_document:
            path = parent.path
            if path is None:
                msg = (
                    "can't create a Datashare file from ES document without path, "
                    "path is needed to locate the file, retrieve it from ES"
                )
                raise ValueError(msg)
            location = FileLocation.FILESYSTEM
        else:
            path = artifacts_dir(doc_id=parent.id, project=parent.project) / "raw"
            location = FileLocation.ARTIFACTS
        # The filesystem dod is alway relative to the base location, let's make sure
        # we store a relative path otherwise joining with the location will fail
        if path.parts and path.parts[0] == os.path.sep:
            path = Path(*path.parts[1:])
        n_pages = 1
        if parent.metadata:
            n_pages = parent.metadata.get("tika_metadata_xmptpg_npages", n_pages)
        path = WorkerFilePath(path=path, location=location)
        return cls(
            doc_id=parent.id,
            parent=parent,
            value=path,
            n_pages=n_pages,
            project=parent.project,
        )


class WorkerFile(
    WithParent["ProcessedFile"], ProcessingResult[WorkerFilePath], ErrorSourceWithPages
):
    """An intermediate results produce by the worker either from a Datashare doc on the
    filesystem or from another intermediate result
    """

    parent: "ProcessedFile"

    def locate(self, paths: WorkerPaths) -> Path:
        return self.value.locate(paths)

    @classmethod
    def from_parent(
        cls, parent: "ProcessedFile", path: Path, paths: WorkerPaths
    ) -> Self:
        path = WorkerFilePath.relative_to_workdir(path, paths)
        return cls(
            doc_id=parent.doc_id,
            project=parent.project,
            parent=parent,
            value=path,
            n_pages=parent.n_pages,
        )


def _file_discriminator(v: Any) -> str:
    parent = v.get("parent") if isinstance(v, dict) else getattr(v, "parent", None)
    if parent is None:
        raise ValueError(f"{v} has no parent")
    if isinstance(parent, dict):
        is_document = parent.get("type") == "Document"
    else:
        is_document = isinstance(parent, Document)
    return "datashare" if is_document else "worker"


ProcessedFile = Annotated[
    Annotated[DatashareFile, Tag("datashare")] | Annotated[WorkerFile, Tag("worker")],
    Discriminator(_file_discriminator),
]
WorkerFile.model_rebuild()
PROCESSED_FILE_TA = TypeAdapter(ProcessedFile, config=BaseModel.model_config)
