# We import everything top level to trigger component registration
from .tasks import (
    FasterWhisperInferenceRunner,
    FasterWhisperPostprocessor,
    FasterWhisperPreprocessor,
    FireRedASR2InferenceRunner,
    FireRedASR2Postprocessor,
    FireRedASR2Preprocessor,
    ParakeetInferenceRunner,
    ParakeetPostprocessor,
    ParakeetPreprocessor,
    ParakeetTrtInferenceRunner,
    WhisperTrtInferenceRunner,
    WhisperTrtPostprocessor,
    WhisperTrtPreprocessor,
)

__all__ = [
    "FasterWhisperInferenceRunner",
    "FasterWhisperPostprocessor",
    "FasterWhisperPreprocessor",
    "FireRedASR2InferenceRunner",
    "FireRedASR2Postprocessor",
    "FireRedASR2Preprocessor",
    "ParakeetInferenceRunner",
    "ParakeetPostprocessor",
    "ParakeetPreprocessor",
    "ParakeetTrtInferenceRunner",
    "WhisperTrtInferenceRunner",
    "WhisperTrtPostprocessor",
    "WhisperTrtPreprocessor",
]
