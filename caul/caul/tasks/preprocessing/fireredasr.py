from caul_core import (
    DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
    DEFAULT_SAMPLE_RATE,
    FIREREDASR2_INFERENCE_MAX_FRAMES,
    ASRModel,
    BaseBatcherConfig,
    Preprocessor,
)

from .asr_preprocessor import ASRPreprocessorMixin


@Preprocessor.register(ASRModel.FIREREDASR2_AED)
class FireRedASR2Preprocessor(ASRPreprocessorMixin):
    def __init__(
        self,
        max_frames: int = FIREREDASR2_INFERENCE_MAX_FRAMES,
        batcher: BaseBatcherConfig | None = None,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
        large_file_threshold_bytes: int = DEFAULT_LARGE_FILE_THRESHOLD_BYTES,
    ):
        super().__init__(
            max_frames=max_frames,
            batcher=batcher,
            sample_rate=sample_rate,
            large_file_threshold_bytes=large_file_threshold_bytes,
        )
