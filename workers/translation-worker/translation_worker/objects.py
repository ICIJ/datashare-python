from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

import langcodes
from datashare_python.objects import (
    DatashareLanguage,
    DatashareModel,
    IETFLanguage,
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
from icij_common.pydantic_utils import make_enum_discriminator, tagged_union
from icij_common.registrable import RegistrableConfig
from pydantic import Discriminator, Field, TypeAdapter

if TYPE_CHECKING:
    from argostranslate.sbd import ISentenceBoundaryDetectionModel

    from .processors import Translator
    from .sentence_splitters import SentenceSplitter


class TorchDevice(StrEnum):
    CPU = "cpu"
    GPU = "cuda"


class SentenceSplitterModel(StrEnum):
    ARGOS = "ARGOS"
    DEFAULT = "DEFAULT"
    RULES = "RULES"
    SAT = "SAT"


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


DocumentSearchQuery = dict[str, Any]
DocId = str


def untranslated_query(target: Language) -> dict:
    query = bool_query(
        must_not({TERM: {f"{DOC_CONTENT_TRANSLATED}.target_language.keyword": target}})
    )
    return query[QUERY]


class _BaseProcessorConfig(DatashareModel, RegistrableConfig): ...


DEFAULT_MAX_SENTENCE_LENGTH = 1024


class SentenceSplitterConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[SentenceSplitterModel]

    # max number of characters per sentence
    max_length: int | None = Field(default=None, gt=0)


class DefaultSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.DEFAULT
    )


class ArgosSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.ARGOS
    )

    sentencizer: ArgosSentencizer = ArgosSentencizer.MINI_SBD


class RulesSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.RULES
    )


DEFAULT_SAT_MODEL_REF = "sat-3l-sm"


class SaTSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.SAT
    )

    model_ref: str = DEFAULT_SAT_MODEL_REF
    device: TorchDevice | None = None


class BaseTranslatorConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[TranslationModel]

    @property
    def max_sentence_length(self) -> int:
        """Max number of source characters per sentence"""
        return DEFAULT_MAX_SENTENCE_LENGTH


_SentenceSplitterConfig = tagged_union(
    SentenceSplitterConfig.__subclasses__(), lambda t: t.model.default
)
splitter_discriminator = make_enum_discriminator("model", SentenceSplitterModel)


class ArgosTranslatorConfig(BaseTranslatorConfig):
    model: ClassVar[TranslationModel] = Field(
        frozen=True, default=TranslationModel.ARGOS
    )

    beam_size: int = 2
    length_penalty: float = 0.2


DEFAULT_HUNYUAN_MODEL_REF = "tencent/HY-MT1.5-1.8B"


class HunyuanMtTranslatorConfig(BaseTranslatorConfig):
    model: ClassVar[TranslationModel] = Field(
        frozen=True, default=TranslationModel.HUNYUAN
    )

    model_ref: str = DEFAULT_HUNYUAN_MODEL_REF
    max_new_tokens: int = 2048
    do_sample: bool = True
    top_k: int = 20
    top_p: float = 0.8
    repetition_penalty: float = 1.05
    temperature: float = 0.7
    torch_dtype: str = "float32"
    device_map: str = "auto"

    @property
    def max_sentence_length(self) -> int:
        # keep a 2x margin so that translations aren't truncated
        return max(1, self.max_new_tokens // 2)


TranslatorConfig = tagged_union(
    BaseTranslatorConfig.__subclasses__(), lambda t: t.model.default
)
translator_discriminator = make_enum_discriminator("model", TranslationModel)


def language_subtag(language: Language) -> str:
    """Normalize a Datashare or IETF language to its IETF language subtag.

    :param language: Datashare language name (``THAI``) or IETF tag (``th``)
    :return: the language subtag, e.g. ``th``
    """
    if isinstance(language, DatashareLanguage):
        return langcodes.get(language.alpha3).language
    return langcodes.get(language).language


def _default_language_sentence_splitters() -> dict[Language, SentenceSplitterConfig]:
    # thai doesn't mark sentence ends with punctuation, needs special splitter
    return {IETFLanguage("th"): SaTSentenceSplitterConfig()}


class TranslationConfig(DatashareModel):
    sentence_splitter: _SentenceSplitterConfig = Field(
        discriminator=Discriminator(splitter_discriminator),
        default_factory=RulesSentenceSplitterConfig,
    )
    # per-source language overrides
    language_sentence_splitters: dict[
        Language,
        Annotated[_SentenceSplitterConfig, Discriminator(splitter_discriminator)],
    ] = Field(default_factory=_default_language_sentence_splitters)
    translator: TranslatorConfig = Field(
        discriminator=Discriminator(translator_discriminator),
        default_factory=ArgosTranslatorConfig,
    )

    def sentence_splitter_for(self, language: Language) -> SentenceSplitterConfig:
        """Get the sentence splitter config of a source language.

        :param language: the source language
        :return: the language's splitter config, or else the default one, with its
            ``max_length`` derived from the translator config when not set
        """
        subtag = language_subtag(language)
        config = next(
            (
                config
                for lang, config in self.language_sentence_splitters.items()
                if language_subtag(lang) == subtag
            ),
            self.sentence_splitter,
        )
        if config.max_length is None:
            max_length = self.translator.max_sentence_length
            config = config.model_copy(update={"max_length": max_length})
        return config

    def to_sentence_splitter(self, language: Language) -> "SentenceSplitter":
        from .processors import SentenceSplitter  # noqa: PLC0415

        return SentenceSplitter.from_config(self.sentence_splitter_for(language))

    def to_translator(self) -> "Translator":
        from .processors import Translator  # noqa: PLC0415

        return Translator.from_config(self.translator)


class TranslationArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery | None = None
    config: "TranslationConfig"
    target_language: Language
    max_docs: int | None = Field(default=None, gt=0)

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
