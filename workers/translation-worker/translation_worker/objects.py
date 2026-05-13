from typing import Annotated, Any

from datashare_python.objects import DatashareModel
from pydantic import BaseModel, BeforeValidator
from pydantic_extra_types.language_code import LanguageName


def _to_language_name(value: Any) -> Any:
    if isinstance(value, str):
        return value.title()
    return value


class TranslationArgs(DatashareModel):
    project: str
    target_language: Annotated[LanguageName, BeforeValidator(_to_language_name)]


class TranslationResponse(DatashareModel):
    n_translations: int = 0


class BatchSentence(BaseModel):
    doc_id: str
    root_document: str
    sentence_index: int
    sentence: str
