from abc import ABC
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self

from icij_common.pydantic_utils import make_enum_discriminator, tagged_union
from icij_common.registrable import RegistrableConfig
from pydantic import Discriminator, Field

from .constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
    DEFAULT_MAX_FRAMES,
    DEFAULT_SAMPLE_RATE,
    FASTER_WHISPER_APPEND_PUNCTUATIONS_DEFAULT,
    FASTER_WHISPER_BEAM_SIZE_DEFAULT,
    FASTER_WHISPER_BEST_OF_DEFAULT,
    FASTER_WHISPER_CLIP_TIMESTAMPS_DEFAULT,
    FASTER_WHISPER_COMPRESSION_RATIO_THRESHOLD_DEFAULT,
    FASTER_WHISPER_COMPUTE_TYPE_DEFAULT,
    FASTER_WHISPER_CONDITION_ON_PREVIOUS_TEXT_DEFAULT,
    FASTER_WHISPER_LARGE_V3_TURBO_SUPPRESSED_TOKENS,
    FASTER_WHISPER_LENGTH_PENALTY_DEFAULT,
    FASTER_WHISPER_LOG_PROB_THRESHOLD_DEFAULT,
    FASTER_WHISPER_MAX_INITIAL_TIMESTAMP_DEFAULT,
    FASTER_WHISPER_MULTILINGUAL_DEFAULT,
    FASTER_WHISPER_NO_REPEAT_NGRAM_SIZE_DEFAULT,
    FASTER_WHISPER_NO_SPEECH_THRESHOLD_DEFAULT,
    FASTER_WHISPER_PATIENCE_DEFAULT,
    FASTER_WHISPER_PREPEND_PUNCTUATIONS_DEFAULT,
    FASTER_WHISPER_PROMPT_RESET_ON_TEMPERATURE_DEFAULT,
    FASTER_WHISPER_REPETITION_PENALTY_DEFAULT,
    FASTER_WHISPER_SUPPRESS_BLANK_DEFAULT,
    FASTER_WHISPER_TEMPERATURES_DEFAULT,
    FASTER_WHISPER_WITHOUT_TIMESTAMPS_DEFAULT,
    FASTER_WHISPER_WORD_TIMESTAMPS_DEFAULT,
    FIREREDASR2_AED_LENGTH_PENALTY_DEFAULT,
    FIREREDASR2_BEAM_SIZE_DEFAULT,
    FIREREDASR2_DECODE_MAX_LEN_DEFAULT,
    FIREREDASR2_EOS_PENALTY_DEFAULT,
    FIREREDASR2_INFERENCE_MAX_FRAMES,
    FIREREDASR2_NBEST_DEFAULT,
    FIREREDASR2_RETURN_TIMESTAMP_DEFAULT,
    FIREREDASR2_SOFTMAX_SMOOTHING_DEFAULT,
    FIREREDASR2_USE_HALF_DEFAULT,
    PARAKEET_INFERENCE_MAX_DURATION_S,
    PARAKEET_INFERENCE_MAX_FRAMES,
    PARAKEET_MODEL_REF,
    WHISPER_TRT_DECODER_BATCH_SIZE,
    WHISPER_TRT_DECODER_BEAM_WIDTH,
    WHISPER_TRT_DECODER_CROSS_ATTENTION,
    WHISPER_TRT_DECODER_DEBUG_MODE,
    WHISPER_TRT_DECODER_GPT_ATTENTION_PLUGIN,
    WHISPER_TRT_DECODER_HAS_POSITION_EMBEDDING,
    WHISPER_TRT_DECODER_HAS_TOKEN_TYPE_EMBEDDING,
    WHISPER_TRT_DECODER_HIDDEN_SIZE,
    WHISPER_TRT_DECODER_NUM_HEADS,
    WHISPER_TRT_DECODER_NUM_HIDDEN_LAYERS,
    WHISPER_TRT_DECODER_PAGED_KV_CACHE,
    WHISPER_TRT_DECODER_REMOVE_INPUT_PADDING,
    WHISPER_TRT_DECODER_VOCAB_SIZE,
    WHISPER_TRT_DTYPE,
    WHISPER_TRT_ENCODER_DOWNSAMPLING_FACTOR,
    WHISPER_TRT_MAX_FRAMES,
    WHISPER_TRT_MAX_MEL_PADDING_LEN,
    WHISPER_TRT_N_MELS,
    WHISPER_TRT_PROMPT_PREFIX,
    WHISPER_TRT_RETURN_TIMESTAMPS,
    TorchDevice,
)
from .objects import ASRModel, BaseModel, BatcherType, FasterWhisperModel

if TYPE_CHECKING:
    with suppress(ImportError):
        from tensorrt_llm.runtime import ModelConfig


class _BaseConfig(BaseModel, RegistrableConfig, ABC): ...


# TensorRT LLM
class TrtLlmEncoderConfig(BaseModel):
    downsampling_factor: int = WHISPER_TRT_ENCODER_DOWNSAMPLING_FACTOR


class TrtLlmDecoderConfig(BaseModel):
    batch_size: int = WHISPER_TRT_DECODER_BATCH_SIZE
    beam_width: int = WHISPER_TRT_DECODER_BEAM_WIDTH
    num_heads: int = WHISPER_TRT_DECODER_NUM_HEADS
    num_kv_heads: int = WHISPER_TRT_DECODER_NUM_HEADS
    hidden_size: int = WHISPER_TRT_DECODER_HIDDEN_SIZE
    vocab_size: int = WHISPER_TRT_DECODER_VOCAB_SIZE
    num_hidden_layers: int = WHISPER_TRT_DECODER_NUM_HIDDEN_LAYERS
    gpt_attention_plugin: str = WHISPER_TRT_DECODER_GPT_ATTENTION_PLUGIN
    remove_input_padding: bool = WHISPER_TRT_DECODER_REMOVE_INPUT_PADDING
    paged_kv_cache: bool = WHISPER_TRT_DECODER_PAGED_KV_CACHE
    has_position_embedding: bool = WHISPER_TRT_DECODER_HAS_POSITION_EMBEDDING
    dtype: str = WHISPER_TRT_DTYPE
    cross_attention: bool = WHISPER_TRT_DECODER_CROSS_ATTENTION
    has_token_type_embedding: bool = WHISPER_TRT_DECODER_HAS_TOKEN_TYPE_EMBEDDING
    debug_mode: bool = WHISPER_TRT_DECODER_DEBUG_MODE

    def to_model_config(self) -> "ModelConfig":
        from tensorrt_llm.llmapi.kv_cache_type import (
            KVCacheType,
        )
        from tensorrt_llm.runtime import (
            ModelConfig,
        )

        return ModelConfig(
            max_batch_size=self.batch_size,
            max_beam_width=self.beam_width,
            num_heads=self.num_heads,
            num_kv_heads=self.num_kv_heads,
            hidden_size=self.hidden_size,
            vocab_size=self.vocab_size,
            cross_attention=self.cross_attention,
            num_hidden_layers=self.num_hidden_layers,
            gpt_attention_plugin=self.gpt_attention_plugin,
            remove_input_padding=self.remove_input_padding,
            kv_cache_type=(
                KVCacheType.PAGED if self.paged_kv_cache else KVCacheType.CONTINUOUS
            ),
            has_position_embedding=self.has_position_embedding,
            dtype=self.dtype,
            has_token_type_embedding=self.has_token_type_embedding,
        )


class BaseBatcherConfig(_BaseConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: ClassVar[BatcherType]


class ConstantSizeBatcherConfig(BaseBatcherConfig):
    type: ClassVar[str] = Field(default=BatcherType.CONSTANT_SIZE)

    batch_size: int = DEFAULT_BATCH_SIZE


class MaxDurationBatcherConfig(ConstantSizeBatcherConfig):
    type: ClassVar[str] = Field(default=BatcherType.MAX_DURATION)

    batch_size: int = 0
    max_duration_s: float = PARAKEET_INFERENCE_MAX_DURATION_S


BatcherConfig = tagged_union(
    [ConstantSizeBatcherConfig, MaxDurationBatcherConfig],
    lambda t: t.type.default.value,
)

batcher_discriminator = make_enum_discriminator("type", BatcherType)


class BasePreprocessorConfig(_BaseConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[ASRModel]

    max_frames: int = DEFAULT_MAX_FRAMES
    sample_rate: int = DEFAULT_SAMPLE_RATE
    large_file_threshold_bytes: int = DEFAULT_LARGE_FILE_THRESHOLD_BYTES

    batcher: BatcherConfig = Field(
        default_factory=ConstantSizeBatcherConfig,
        discriminator=Discriminator(batcher_discriminator),
    )


class BaseInferenceRunnerConfig(_BaseConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[ASRModel]

    tmp_dir_fallback: bool = False


class BasePostprocessorConfig(_BaseConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="model")
    model: ClassVar[ASRModel]


# Parakeet
class ParakeetPreprocessorConfig(BasePreprocessorConfig):
    model: ClassVar[str] = Field(default=ASRModel.PARAKEET)

    max_frames: int = PARAKEET_INFERENCE_MAX_FRAMES


class ParakeetInferenceRunnerConfig(BaseInferenceRunnerConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.PARAKEET)
    model_name: str = PARAKEET_MODEL_REF


class ParakeetTrtInferenceRunnerConfig(BaseInferenceRunnerConfig):
    model_path: Path | str = None
    engine_path: Path | str = None
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.PARAKEET_TRT)


class ParakeetPostprocessorConfig(BasePostprocessorConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.PARAKEET)


# Faster Whisper


class FasterWhisperPreprocessorConfig(BasePreprocessorConfig):
    model: ClassVar[str] = Field(default=ASRModel.FASTER_WHISPER)


class FasterWhisperInferenceRunnerConfig(BaseInferenceRunnerConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.FASTER_WHISPER)

    whisper_model_name: FasterWhisperModel = FasterWhisperModel.MEDIUM

    compute_type: str = FASTER_WHISPER_COMPUTE_TYPE_DEFAULT
    word_timestamps: bool = FASTER_WHISPER_WORD_TIMESTAMPS_DEFAULT
    beam_size: int = FASTER_WHISPER_BEAM_SIZE_DEFAULT
    best_of: int = FASTER_WHISPER_BEST_OF_DEFAULT
    patience: int = FASTER_WHISPER_PATIENCE_DEFAULT
    length_penalty: int = FASTER_WHISPER_LENGTH_PENALTY_DEFAULT
    repetition_penalty: int = FASTER_WHISPER_REPETITION_PENALTY_DEFAULT
    no_repeat_ngram_size: int = FASTER_WHISPER_NO_REPEAT_NGRAM_SIZE_DEFAULT
    log_prob_threshold: float = FASTER_WHISPER_LOG_PROB_THRESHOLD_DEFAULT
    no_speech_threshold: float = FASTER_WHISPER_NO_SPEECH_THRESHOLD_DEFAULT
    compression_ratio_threshold: float = (
        FASTER_WHISPER_COMPRESSION_RATIO_THRESHOLD_DEFAULT
    )
    condition_on_previous_text: bool = FASTER_WHISPER_CONDITION_ON_PREVIOUS_TEXT_DEFAULT
    prompt_reset_on_temperature: float = (
        FASTER_WHISPER_PROMPT_RESET_ON_TEMPERATURE_DEFAULT
    )
    temperatures: list[float] = FASTER_WHISPER_TEMPERATURES_DEFAULT
    initial_prompt: str | None = None
    prefix: str | None = None
    suppress_blank: bool = FASTER_WHISPER_SUPPRESS_BLANK_DEFAULT
    suppress_tokens: list[int] = FASTER_WHISPER_LARGE_V3_TURBO_SUPPRESSED_TOKENS
    without_timestamps: bool = FASTER_WHISPER_WITHOUT_TIMESTAMPS_DEFAULT
    max_initial_timestamp: float = FASTER_WHISPER_MAX_INITIAL_TIMESTAMP_DEFAULT
    prepend_punctuations: str = FASTER_WHISPER_PREPEND_PUNCTUATIONS_DEFAULT
    append_punctuations: str = FASTER_WHISPER_APPEND_PUNCTUATIONS_DEFAULT
    multilingual: bool = FASTER_WHISPER_MULTILINGUAL_DEFAULT
    max_new_tokens: int | None = None
    clip_timestamps: list[float] = FASTER_WHISPER_CLIP_TIMESTAMPS_DEFAULT
    hallucination_silence_threshold: float | None = None
    hotwords: str | None = None


class FasterWhisperPostprocessorConfig(BasePostprocessorConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.FASTER_WHISPER)


# FireRedASR2s


class FireRedASR2PreprocessorConfig(BasePreprocessorConfig):
    model: ClassVar[str] = Field(default=ASRModel.FIREREDASR2_AED)

    max_frames: int = FIREREDASR2_INFERENCE_MAX_FRAMES


class FireRedASR2InferenceRunnerConfig(BaseInferenceRunnerConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.FIREREDASR2_AED)

    use_half: bool = FIREREDASR2_USE_HALF_DEFAULT
    beam_size: int = FIREREDASR2_BEAM_SIZE_DEFAULT
    nbest: int = FIREREDASR2_NBEST_DEFAULT
    decode_max_len: int = FIREREDASR2_DECODE_MAX_LEN_DEFAULT
    softmax_smoothing: float = FIREREDASR2_SOFTMAX_SMOOTHING_DEFAULT
    aed_length_penalty: float = FIREREDASR2_AED_LENGTH_PENALTY_DEFAULT
    eos_penalty: float = FIREREDASR2_EOS_PENALTY_DEFAULT
    return_timestamp: bool = FIREREDASR2_RETURN_TIMESTAMP_DEFAULT


class FireRedASR2PostprocessorConfig(BasePostprocessorConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.FIREREDASR2_AED)


# Whisper TRT


class WhisperTrtPreprocessorConfig(BasePreprocessorConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.WHISPER_TRT)

    n_mels: int = WHISPER_TRT_N_MELS
    mel_filters_dir: str | None = None
    dtype: str = WHISPER_TRT_DTYPE
    max_frames: int = WHISPER_TRT_MAX_FRAMES


class WhisperTrtInferenceRunnerConfig(BaseInferenceRunnerConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.WHISPER_TRT)

    encoder_config: TrtLlmEncoderConfig = Field(default_factory=TrtLlmEncoderConfig)
    decoder_config: TrtLlmDecoderConfig = Field(default_factory=TrtLlmDecoderConfig)

    encoder_path: str
    decoder_path: str

    prompt_prefix: str = WHISPER_TRT_PROMPT_PREFIX
    return_timestamps: bool = WHISPER_TRT_RETURN_TIMESTAMPS
    max_mel_padding_len: int = WHISPER_TRT_MAX_MEL_PADDING_LEN


class WhisperTrtPostprocessorConfig(BasePostprocessorConfig):
    model: ClassVar[str] = Field(frozen=True, default=ASRModel.WHISPER_TRT)


PreprocessorConfig = tagged_union(
    BasePreprocessorConfig.__subclasses__(), lambda t: t.model.default.value
)

InferenceRunnerConfig = tagged_union(
    BaseInferenceRunnerConfig.__subclasses__(), lambda t: t.model.default.value
)

PostprocessorConfig = tagged_union(
    BasePostprocessorConfig.__subclasses__(), lambda t: t.model.default.value
)


model_discriminator = make_enum_discriminator("model", ASRModel)


class ASRPipelineConfig(BaseModel):
    device: TorchDevice = TorchDevice.CPU
    preprocessing: PreprocessorConfig = Field(
        discriminator=Discriminator(model_discriminator),
    )
    inference: InferenceRunnerConfig = Field(
        discriminator=Discriminator(model_discriminator),
    )
    postprocessing: PostprocessorConfig = Field(
        discriminator=Discriminator(model_discriminator),
    )

    @classmethod
    def parakeet(cls, device: TorchDevice = TorchDevice.CPU) -> Self:
        return cls(
            device=device,
            preprocessing=ParakeetPreprocessorConfig(),
            inference=ParakeetInferenceRunnerConfig(),
            postprocessing=ParakeetPostprocessorConfig(),
        )

    @classmethod
    def parakeet_trt(cls, device: TorchDevice = TorchDevice.CPU) -> Self:
        return cls(
            device=device,
            preprocessing=ParakeetPreprocessorConfig(),
            inference=ParakeetTrtInferenceRunnerConfig(),
            postprocessing=ParakeetPostprocessorConfig(),
        )

    @classmethod
    def fireredasr2(
        cls, *, tmp_dir_fallback: bool = True, device: TorchDevice = TorchDevice.CPU
    ) -> Self:
        return cls(
            device=device,
            preprocessing=FireRedASR2PreprocessorConfig(),
            inference=FireRedASR2InferenceRunnerConfig(
                tmp_dir_fallback=tmp_dir_fallback
            ),
            postprocessing=FireRedASR2PostprocessorConfig(),
        )

    @classmethod
    def faster_whisper(cls, device: TorchDevice = TorchDevice.CPU) -> Self:
        return cls(
            device=device,
            preprocessing=FasterWhisperPreprocessorConfig(),
            inference=FasterWhisperInferenceRunnerConfig(),
            postprocessing=FasterWhisperPostprocessorConfig(),
        )

    @classmethod
    def whisper_trt(
        cls,
        encoder_path: str,
        *,
        decoder_path: str,
        device: TorchDevice = TorchDevice.CPU,
    ) -> Self:
        return cls(
            device=device,
            preprocessing=WhisperTrtPreprocessorConfig(),
            inference=WhisperTrtInferenceRunnerConfig(
                encoder_path=encoder_path, decoder_path=decoder_path
            ),
            postprocessing=WhisperTrtPostprocessorConfig(),
        )
