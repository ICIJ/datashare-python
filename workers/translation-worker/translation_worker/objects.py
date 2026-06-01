from abc import ABC
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from datashare_python.objects import (
    BaseModel,
    DatashareModel,
    Language,
)
from icij_common.es import (
    DOC_CONTENT_TRANSLATED,
    QUERY,
    TERM,
    bool_query,
    has_id,
    must_not,
)
from icij_common.registrable import RegistrableConfig
from pydantic import Field

from .processors import SentenceSplitter, Translator

if TYPE_CHECKING:
    from argostranslate.sbd import ISentenceBoundaryDetectionModel


class _BaseProcessorConfig(BaseModel, RegistrableConfig, ABC): ...


class SentenceSplitterModel(StrEnum):
    ARGOS = "ARGOS"


class TranslationModel(StrEnum):
    ARGOS = "ARGOS"


class SentenceSplitterConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[SentenceSplitterModel]


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


class ArgosSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = SentenceSplitterModel.ARGOS

    sentencizer: ArgosSentencizer = ArgosSentencizer.MINI_SBD


class TranslatorConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[TranslationModel]


class ArgosTranslatorConfig(TranslatorConfig):
    model: ClassVar[TranslationModel] = TranslationModel.ARGOS

    beam_size: int = 2
    length_penalty: float = 0.2


# TODO: uncomment when adding more implems
# _SentenceSplitterConfig = tagged_union(
#     SentenceSplitterConfig.__subclasses__(), lambda t: t.model.default.value
# )
# splitter_discriminator = make_enum_discriminator("model", SentenceSplitterModel)

# TODO: uncomment when adding more implems
# _TranslatorConfig = tagged_union(
#     TranslatorConfig.__subclasses__(), lambda t: t.model.default.value
# )
# translator_discriminator = make_enum_discriminator("model", TranslationModel)


class TranslationConfig(DatashareModel):
    sentence_splitter: ArgosSentenceSplitterConfig = Field(
        # TODO: uncomment when adding more implem
        # discriminator=Discriminator(model_discriminator=splitter_discriminator),
        default_factory=ArgosSentenceSplitterConfig,
    )
    translator: ArgosTranslatorConfig = Field(
        # discriminator=Discriminator(model_discriminator=splitter_discriminator),
        default_factory=ArgosTranslatorConfig,
    )

    def to_sentence_splitter(self) -> "SentenceSplitter":
        from .processors import SentenceSplitter  # noqa: PLC0415

        return SentenceSplitter.from_config(self.sentence_splitter)

    def to_translator(self) -> "Translator":
        from .processors import Translator  # noqa: PLC0415

        return Translator.from_config(self.translator)


DocumentSearchQuery = dict[str, Any]
DocId = str


def untranslated_query(target: Language) -> dict:
    query = bool_query(
        must_not({TERM: {f"{DOC_CONTENT_TRANSLATED}.target_language.keyword": target}})
    )
    return query[QUERY]


class TranslationArgs(DatashareModel):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None = None
    config: TranslationConfig = Field(default_factory=TranslationConfig)
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
