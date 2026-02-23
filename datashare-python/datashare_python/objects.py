import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum, unique
from typing import Any, Literal, Self

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID, ID_, SOURCE
from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    merge_configs,
    no_enum_values_config,
)
from pydantic import BaseModel as _BaseModel
from pydantic import Field
from pydantic.main import IncEx

logger = logging.getLogger(__name__)


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


class Document(DatashareModel):
    id: str
    root_document: str
    content: str
    language: str
    tags: list[str] = Field(default_factory=list)
    content_translated: dict[str, str] = Field(
        default_factory=dict, alias="content_translated"
    )

    @classmethod
    def from_es(cls, es_doc: dict) -> Self:
        sources = es_doc[SOURCE]
        return cls(
            id=es_doc[ID_],
            content=sources[DOC_CONTENT],
            content_translated=sources.get("content_translated", dict()),
            language=sources[DOC_LANGUAGE],
            root_document=sources[DOC_ROOT_ID],
            tags=sources.get("tags", []),
        )
