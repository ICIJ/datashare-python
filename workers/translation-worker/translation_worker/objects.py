from enum import StrEnum
from typing import TYPE_CHECKING, Any

from datashare_python.objects import (
    DatashareModel,
    Language,
    TaskArgs,
)
from icij_common.es import (
    DOC_CONTENT_TRANSLATED,
    QUERY,
    TERM,
    bool_query,
    has_id,
    must_not,
)
from pydantic import TypeAdapter

from .constants import HUNYUAN_LANGUAGES

if TYPE_CHECKING:
    from argostranslate.sbd import ISentenceBoundaryDetectionModel

    from translation_worker.objects import TranslationConfig


class SentenceSplitterModel(StrEnum):
    ARGOS = "ARGOS"
    DEFAULT = "DEFAULT"


class TranslationModel(StrEnum):
    ARGOS = "ARGOS"
    HUNYUAN = "HUNYUAN"


class ArgosSentencizer(StrEnum):
    SPACY_SMALL = "spacy_small"
    MINI_SBD = "mini_sbd"

    @property
    def sentencizer_cls(self) -> type["ISentenceBoundaryDetectionModel"]:
        from argostranslate.sbd import (  # noqa: PLC0415
            MiniSBDSentencizer,
            SpacySentencizerSmall,
        )

        match self:
            case ArgosSentencizer.SPACY_SMALL:
                return SpacySentencizerSmall
            case ArgosSentencizer.MINI_SBD:
                return MiniSBDSentencizer
            case _:
                raise NotImplementedError()


_LANGUAGE_TYPE_ADAPTER = TypeAdapter(Language)

_VALIDATED_HUNYUAN_LANGUAGES = {
    _LANGUAGE_TYPE_ADAPTER.validate_python(lang) for lang in HUNYUAN_LANGUAGES
}


DocumentSearchQuery = dict[str, Any]
DocId = str


def untranslated_query(target: Language) -> dict:
    query = bool_query(
        must_not({TERM: {f"{DOC_CONTENT_TRANSLATED}.target_language.keyword": target}})
    )
    return query[QUERY]


class TranslationArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None = None
    config: "TranslationConfig"
    target_language: Language

    def as_query(self) -> dict[str, Any]:
        match self.docs:
            case None:
                return untranslated_query(self.target_language)
            case list():
                return has_id(self.docs)
            case dict():
                return self.docs
            case _:
                raise ValueError(f"unsupported docs {self.docs}")


class TranslationResponse(DatashareModel):
    n_translations: int = 0
