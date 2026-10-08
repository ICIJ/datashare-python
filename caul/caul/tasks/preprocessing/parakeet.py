from typing import Self

from caul_core import (
    DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
    DEFAULT_SAMPLE_RATE,
    PARAKEET_INFERENCE_MAX_DURATION_S,
    PARAKEET_INFERENCE_MAX_FRAMES,
    ASRModel,
    BaseBatcherConfig,
    ParakeetPreprocessorConfig,
    Preprocessor,
)

from .asr_preprocessor import ASRPreprocessorMixin


@Preprocessor.register(ASRModel.PARAKEET)
class ParakeetPreprocessor(ASRPreprocessorMixin):
    def __init__(
        self,
        max_frames: int = PARAKEET_INFERENCE_MAX_FRAMES,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
        large_file_threshold_bytes: int = DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
        max_duration_s: float = PARAKEET_INFERENCE_MAX_DURATION_S,
        batcher: BaseBatcherConfig | None = None,
    ):
        super().__init__(
            max_frames=max_frames,
            batcher=batcher,
            sample_rate=sample_rate,
            large_file_threshold_bytes=large_file_threshold_bytes,
        )
        self._max_duration_s = max_duration_s

    @classmethod
    def _from_config(cls, config: ParakeetPreprocessorConfig, **extras) -> Self:
        return cls(
            max_frames=config.max_frames,
            batcher=config.batcher,
            sample_rate=config.sample_rate,
            large_file_threshold_bytes=config.large_file_threshold_bytes,
        )
