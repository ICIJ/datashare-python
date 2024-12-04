from typing import Self

from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID, ID_, SOURCE
from icij_common.pydantic_utils import ICIJModel, LowerCamelCaseModel
from pydantic import Field


class Document(LowerCamelCaseModel):
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


class ClassificationConfig(ICIJModel):
    task: str = Field(const=True, default="text-classification")
    model: str = None
    batch_size: int = 16


class TranslationConfig(ICIJModel):
    task: str = Field(const=True, default="translation")
    batch_size: int = 16
    _base_model = "Helsinki-NLP/opus-mt"

    def to_pipeline_args(self, source_language: str, *, target_language: str) -> dict:
        as_dict = self.dict()
        as_dict["task"] = f"translation_{source_language}_to_{target_language}"
        as_dict["model"] = f"{self._base_model}-{source_language}-{target_language}"
        return as_dict
