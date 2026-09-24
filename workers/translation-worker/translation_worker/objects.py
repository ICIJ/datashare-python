from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

from datashare_python.objects import DatashareModel, Language, TaskArgs
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


class SentenceSplitterConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[SentenceSplitterModel]


class DefaultSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.DEFAULT
    )


class ArgosSentenceSplitterConfig(SentenceSplitterConfig):
    model: ClassVar[SentenceSplitterModel] = Field(
        frozen=True, default=SentenceSplitterModel.ARGOS
    )

    sentencizer: ArgosSentencizer = ArgosSentencizer.MINI_SBD


class BaseTranslatorConfig(_BaseProcessorConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[TranslationModel]


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


DEFAULT_HUNYUAN_MODEL_REF = "tencent/Hunyuan-MT-Chimera-7B"


class HunyuanMtTranslatorConfig(BaseTranslatorConfig):
    model: ClassVar[TranslationModel] = Field(
        frozen=True, default=TranslationModel.HUNYUAN
    )

    model_ref: str = DEFAULT_HUNYUAN_MODEL_REF
    max_new_tokens: int = 2048
    top_k: int = 20
    top_p: float = 0.6
    repetition_penalty: float = 1.05
    temperature: float = 0.7
    torch_dtype: str = "float32"
    device_map: str = "auto"


TranslatorConfig = tagged_union(
    BaseTranslatorConfig.__subclasses__(), lambda t: t.model.default
)
translator_discriminator = make_enum_discriminator("model", TranslationModel)


class TranslationConfig(DatashareModel):
    sentence_splitter: _SentenceSplitterConfig = Field(
        discriminator=Discriminator(splitter_discriminator),
        default_factory=DefaultSentenceSplitterConfig,
    )
    translator: TranslatorConfig = Field(
        discriminator=Discriminator(translator_discriminator),
        default_factory=ArgosTranslatorConfig,
    )

    def to_sentence_splitter(self) -> "SentenceSplitter":
        from .processors import SentenceSplitter  # noqa: PLC0415

        return SentenceSplitter.from_config(self.sentence_splitter)

    def to_translator(self) -> "Translator":
        from .processors import Translator  # noqa: PLC0415

        return Translator.from_config(self.translator)


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
