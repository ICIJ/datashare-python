import pycountry
from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    merge_configs,
)
from pydantic import BaseModel, Field


class BasePayload(BaseModel):
    model_config = icij_config()


class LowerCamelCaseModel(BaseModel):
    model_config = merge_configs(icij_config(), lowercamel_case_config())


class ClassificationConfig(BasePayload):
    task: str = Field(default="text-classification", frozen=True)
    model: str = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
    batch_size: int = 16
    batches_per_task: int = 5


class TranslationConfig(BasePayload):
    task: str = Field(default="translation", frozen=True)
    model: str = "Helsinki-NLP/opus-mt"
    batch_size: int = 16

    def to_pipeline_args(self, source_language: str, *, target_language: str) -> dict:
        as_dict = self.model_dump()
        source_alpha2 = pycountry.languages.get(name=source_language).alpha_2
        target_alpha2 = pycountry.languages.get(name=target_language).alpha_2
        as_dict["task"] = f"translation_{source_alpha2}_to_{target_alpha2}"
        as_dict["model"] = f"{self.model}-{source_alpha2}-{target_alpha2}"
        return as_dict


class TranslateAndClassifyConfig(BasePayload):
    translation: TranslationConfig = TranslationConfig()
    classification: ClassificationConfig = ClassificationConfig()


class TranslateAndClassifyRequest(BasePayload):
    project: str
    language: str
    config: TranslateAndClassifyConfig = TranslateAndClassifyConfig()


class TranslateAndClassifyResponse(BasePayload):
    translated: int
    classified: int
