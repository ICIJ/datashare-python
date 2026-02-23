from typing import Self, LiteralString

import logging

from datetime import datetime
from enum import Enum, unique

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID, ID_, SOURCE
from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    merge_configs,
    no_enum_values_config,
)
from icij_common.registrable import Registrable

import pycountry
from pydantic import BaseModel as _BaseModel, Field

logger = logging.getLogger(__name__)


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())


class DatashareModel(BaseModel):
    model_config = merge_configs(BaseModel.model_config, lowercamel_case_config())


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


class ClassificationConfig(BaseModel):
    task: LiteralString = Field(const=True, default="text-classification")
    model: str = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
    batch_size: int = 16


class TranslationConfig(BaseModel):
    task: LiteralString = Field(const=True, default="translation")
    model: str = "Helsinki-NLP/opus-mt"
    batch_size: int = 16

    def to_pipeline_args(self, source_language: str, *, target_language: str) -> dict:
        as_dict = self.dict()
        source_alpha2 = pycountry.languages.get(name=source_language).alpha_2
        target_alpha2 = pycountry.languages.get(name=target_language).alpha_2
        as_dict["task"] = f"translation_{source_alpha2}_to_{target_alpha2}"
        as_dict["model"] = f"{self.model}-{source_alpha2}-{target_alpha2}"
        return as_dict


@unique
class TaskState(str, Enum):
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


class Message(DatashareModel, Registrable):
    type: str = Field(frozen=True, alias="@type")


class TaskResult(Message):
    value: object


class TaskError(Message):
    name: str
    message: str
    cause: str | None = None
    stacktrace: list[StacktraceItem] = Field(default_factory=list)


class Task(Message):
    id: str
    name: str
    args: dict[str, object] | None = None
    state: TaskState
    result: TaskResult | None = None
    error: TaskError | None = None
    progress: float | None = None
    created_at: datetime
    completed_at: datetime | None = None
    retries_left: int | None = None
    max_retries: int | None = None
