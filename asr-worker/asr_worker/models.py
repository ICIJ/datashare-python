import math
from pathlib import Path
from typing import Self

from caul.model_handlers.objects import ASRModelHandlerResult
from datashare_python.objects import DatashareModel
from pydantic import Field

from .constants import PARAKEET


class _WithBatchSize(DatashareModel):
    """Batch size helper"""

    batch_size: int = 32


# TODO: ideally these should be caul objects from which can directly init components
#  using from_config
class PreprocessingConfig(DatashareModel):
    """Preprocessing config"""


class InferenceConfig(DatashareModel):
    """Inference config"""

    model_name: str = PARAKEET


class ASRPipelineConfig(DatashareModel):
    batch_size: int = 32
    preprocessing: ParakeetPreprocessorConfig = Field(
        default_factory=ParakeetPreprocessorConfig
    )
    inference: InferenceRunnerConfig = Field(
        default_factory=ParakeetInferenceRunnerConfig
    )
    postprocessing: PostProcessorConfig = Field(
        default_factory=ParakeetPostprocessorConfig
    )


class ASRInputs(DatashareModel):
    project: str
    paths: list[Path]
    config: ASRPipelineConfig


class ASRResponse(DatashareModel):
    n_transcribed: int


class Timestamp(DatashareModel):
    start_s: float
    end_s: float

    @classmethod
    def from_floats(cls, start_s: float, end_s: float) -> Self:
        return Timestamp(start_s=start_s, end_s=end_s)


class Transcript(DatashareModel):
    text: str
    timestamp: Timestamp | None = None
    speaker: str | None = None


class Transcription(DatashareModel):
    transcripts: list[Transcript] = Field(default_factory=list)
    # TODO: add validation [0, 1]
    confidence: float

    @classmethod
    def from_asr_handler_result(cls, asr_handler_result: ASRModelHandlerResult) -> Self:
        transcripts = [
            Transcript(text=text, timestamp=Timestamp(start_s=start_s, end_s=end_s))
            for start_s, end_s, text in asr_handler_result.transcription
        ]
        confidence = asr_handler_result.score
        if confidence is not None:
            confidence = math.exp(asr_handler_result.score)
        return Transcription(confidence=confidence, transcripts=transcripts)
