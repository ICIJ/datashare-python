from abc import ABC
from typing import TYPE_CHECKING, ClassVar

from datashare_python.config import ResourceCacheConfig, WorkerConfig
from datashare_python.objects import BaseModel, DatashareModel
from icij_common.pydantic_utils import make_enum_discriminator, tagged_union
from icij_common.registrable import RegistrableConfig
from pydantic import Discriminator, Field

from .objects import (
    ArgosSentencizer,
    SentenceSplitterModel,
    TorchDevice,
    TranslationModel,
)

if TYPE_CHECKING:
    from translation_worker.processors import SentenceSplitter, Translator

DEFAULT_HUNYUAN_MODEL_REF = "tencent/Hunyuan-MT-Chimera-7B"


class _BaseProcessorConfig(BaseModel, RegistrableConfig, ABC): ...


class TranslationCache(BaseModel):
    sentence_splitter: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )
    translator: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )


class C2TranslateConfig(DatashareModel):
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization


class TranslationWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    batch_size: int = 16
    batch_text_length: int = 10000
    batches_per_worker: int = 10
    es_buffer_size: int = 10

    cache: TranslationCache = Field(default_factory=TranslationCache)

    c2_translate: C2TranslateConfig = Field(default_factory=C2TranslateConfig)


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


WORKER_CONFIG_CLS = TranslationWorkerConfig

# Allows us to get around a circular import with objects.py

from .objects import TranslationArgs  # noqa: E402

TranslationArgs.model_rebuild(_types_namespace={"TranslationConfig": TranslationConfig})
