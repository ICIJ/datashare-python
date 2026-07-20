import logging
import os
from abc import ABC
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, unique
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any, ClassVar, Generic, Literal, Self, TypeVar, cast

import langcodes
from icij_common.registrable import Registrable
from pydantic_core import PydanticCustomError, ValidationError, core_schema
from pydantic_core.core_schema import PlainValidatorFunctionSchema
from pydantic_extra_types.language_code import LanguageName
from temporalio import workflow

from .constants import TIKA_METADATA_RESOURCENAME

with workflow.unsafe.imports_passed_through():
    from icij_common.es import (
        DOC_CONTENT,
        DOC_CONTENT_TRANSLATED,
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
    TypeAdapter,
    model_validator,
)
from pydantic import BaseModel as _BaseModel
from pydantic.main import IncEx

logger = logging.getLogger(__name__)


T = TypeVar("T")
Predicate = Callable[[T], bool] | Callable[[T], Awaitable[bool]]


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())


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
class DocumentLocation(StrEnum):
    ORIGINAL = "original"
    ARTIFACTS = "artifacts"
    WORKDIR = "workdir"


def _is_relative(value: Path) -> Path:
    if value.is_absolute():
        raise ValueError(
            f"FilesystemDocument path should always be relative, found {value}"
        )
    return value


class FilesystemDocument(DatashareModel):
    id: str
    path: Annotated[Path, AfterValidator(_is_relative)]
    index: str
    location: DocumentLocation
    resource_name: str

    def locate(
        self, original_root: Path, *, artifacts_root: Path, workdir: Path
    ) -> Path:
        from datashare_python.utils import artifacts_dir  # noqa: PLC0415

        match self.location:
            case DocumentLocation.ORIGINAL:
                return original_root / self.path
            case DocumentLocation.ARTIFACTS:
                project = self.index
                return artifacts_root / artifacts_dir(self.id, project=project) / "raw"
            case DocumentLocation.WORKDIR:
                return workdir / self.path
            case _:
                raise ValueError(f"invalid location: {self.path}")


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


Routing = str
DocID = str
DocRoute = tuple[DocID, Routing]


class Document(DatashareModel):
    id: str
    language: DatashareLanguage
    index: str | None = None
    root_document: str | None = None
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

    @classmethod
    def from_es(cls, es_doc: dict) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            id=es_doc[ID_],
            index=es_doc.get(INDEX_),
            content=sources.get(DOC_CONTENT),
            content_translated=sources.get(DOC_CONTENT_TRANSLATED, []),
            content_text_length=sources.get("content_text_length"),
            language=DatashareLanguage(sources[DOC_LANGUAGE]),
            root_document=sources.get(DOC_ROOT_ID),
            tags=sources.get("tags", []),
            path=sources.get(DOC_PATH),
            metadata=sources.get(DOC_METADATA),
        )

    def to_filesystem(self) -> FilesystemDocument:
        from .utils import artifacts_dir  # noqa: PLC0415

        if self.metadata is None:
            raise ValueError(
                "can't compute filesyste path for document withtout metadata"
            )
        resource_name = cast(str, self.metadata[TIKA_METADATA_RESOURCENAME])
        if self.root_document is None:
            path = self.path
            location = DocumentLocation.ORIGINAL
        else:
            if self.index is None:
                msg = (
                    f"can't compute filesystem path for embedded doc {self.id} without"
                    f" index"
                )
                raise ValueError(msg)
            path = artifacts_dir(doc_id=self.id, project=self.index) / "raw"
            location = DocumentLocation.ARTIFACTS
        # The filesystem dod is alway relative to the base location, let's make sure
        # we store a relative path otherwise joining with the location will fail
        if path.parts and path.parts[0] == os.path.sep:
            path = Path(*path.parts[1:])
        return FilesystemDocument(
            id=self.id,
            path=path,
            index=self.index,
            location=location,
            resource_name=resource_name,
        )

    def to_route(self) -> DocRoute:
        if self.root_document is not None:
            return self.root_document, self.id
        return self.id, self.id


def _is_absolute_path(v: bytes | BytesIO | Path) -> Any:
    if isinstance(v, Path) and not v.is_absolute():
        raise ValueError("artifact path must be absolute")
    return v


class ArtifactType(StrEnum):
    STRUCTURE = "structure"
    ASR_TRANSCRIPTION = "transcription"


class ManifestEntryStatus(StrEnum):
    COMPLETE = "complete"


class TaskArgs(DatashareModel, ABC):
    def as_manifest_task_input(self) -> dict[str, Any]:
        # This is a base implementation, if the input is too large to be dumped,
        # override this and pop large keys
        as_manifest = self.model_dump(by_alias=True)
        return as_manifest


A = TypeVar("A", bound=TaskArgs)


class ManifestEntry(DatashareModel, Generic[A], ABC):
    status: ManifestEntryStatus
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
        return cls(
            input=args.as_manifest_task_input(),
            label=label,
            status=ManifestEntryStatus.COMPLETE,
            **kwargs,
        )


class PaginationType(StrEnum):
    FILESYSTEM = "filesystem"
    BYTE_RANGES = "byteRanges"


class BasePagination(DatashareModel, Registrable, ABC):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")

    total: int
    type: ClassVar[PaginationType] = Field(frozen=True)


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

    @model_validator(mode="after")
    def byte_ranges_length_should_match_total(self) -> Self:
        if len(self.byte_ranges) != self.total:
            msg = (
                f"byte_ranges must match total. Found {len(self.byte_ranges)} for"
                f" byte_ranges and  {self.total} for total."
            )
            raise ValueError(msg)
        return self


pagination_discriminator = make_enum_discriminator("type", PaginationType)
Pagination = Annotated[
    tagged_union(BasePagination.__subclasses__(), lambda x: x.type.default),
    Discriminator(pagination_discriminator),
]


class DocArtifact(BaseModel, ABC):
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
