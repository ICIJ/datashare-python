import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, unique
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any, Literal, Self, TypeVar, cast

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
    merge_configs,
    no_enum_values_config,
)
from pydantic import AfterValidator, Field
from pydantic import BaseModel as _BaseModel
from pydantic.main import IncEx

logger = logging.getLogger(__name__)


T = TypeVar("T")
Predicate = Callable[[T], bool] | Callable[[T], Awaitable[bool]]


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())


class DatashareModel(BaseModel):
    model_config = merge_configs(BaseModel.model_config, lowercamel_case_config())


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


class Document(DatashareModel):
    id: str
    language: str
    index: str | None = None
    root_document: str | None = None
    content: str | None = None
    content_type: str | None = None
    path: Path | None = None
    tags: list[str] = Field(default_factory=list)
    content_translated: dict[str, str] = Field(
        default_factory=dict, alias="content_translated"
    )
    metadata: dict[str, Any] | None = None
    type: str = Field(default="Document", frozen=True)

    @classmethod
    def from_es(cls, es_doc: dict) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            id=es_doc[ID_],
            index=es_doc.get(INDEX_),
            content=sources.get(DOC_CONTENT),
            content_translated=sources.get(DOC_CONTENT_TRANSLATED, dict()),
            language=sources[DOC_LANGUAGE],
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


@dataclass(frozen=True)
class DocArtifact:
    project: str
    doc_id: str
    artifact: bytes | BytesIO
    filename: str
    metadata_key: str
