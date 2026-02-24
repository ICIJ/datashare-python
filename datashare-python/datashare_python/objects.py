from typing import Self, LiteralString

import pycountry
from icij_common.es import DOC_CONTENT, DOC_LANGUAGE, DOC_ROOT_ID, ID_, SOURCE
from icij_common.pydantic_utils import lowercamel_case_config, icij_config
from pydantic import Field, ConfigDict


class Document:
    id: str
    root_document: str
    content: str
    language: str
    tags: list[str] = Field(default_factory=list)
    content_translated: dict[str, str] = Field(
        default_factory=dict, alias="content_translated"
    )
    model_config: ConfigDict = lowercamel_case_config()

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


class ClassificationConfig:
    task: LiteralString = Field(default="text-classification")
    model: str = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
    batch_size: int = 16
    model_config: ConfigDict = icij_config()


class TranslationConfig:
    task: LiteralString = Field(default="translation")
    model: str = "Helsinki-NLP/opus-mt"
    batch_size: int = 16
    model_config: ConfigDict = icij_config()

    def to_pipeline_args(self, source_language: str, *, target_language: str) -> dict:
        as_dict = self.dict()
        source_alpha2 = pycountry.languages.get(name=source_language).alpha_2
        target_alpha2 = pycountry.languages.get(name=target_language).alpha_2
        as_dict["task"] = f"translation_{source_alpha2}_to_{target_alpha2}"
        as_dict["model"] = f"{self.model}-{source_alpha2}-{target_alpha2}"
        return as_dict
