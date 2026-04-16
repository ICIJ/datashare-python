import math
from typing import Annotated, Any, Self

from caul.asr_pipeline import ASRPipelineConfig
from caul.config import InferenceRunnerConfig as CaulInferenceRunnerConfig
from caul.constant import ASRModel
from caul.objects import ASRResult
from caul.tasks import (
    ParakeetInferenceRunnerConfig,
    ParakeetPostprocessorConfig,
    ParakeetPreprocessorConfig,
)
from datashare_python.objects import DatashareModel
from icij_common.pydantic_utils import make_enum_discriminator, tagged_union
from pydantic import Discriminator, Field

model_discriminator = make_enum_discriminator("model", ASRModel)
InferenceRunnerConfig = Annotated[
    tagged_union(
        CaulInferenceRunnerConfig.__subclasses__(), lambda t: t.model.default.value
    ),
    Discriminator(model_discriminator),
]


_DEFAULT_PIPELINE_CONFIG = ASRPipelineConfig(
    preprocessing=ParakeetPreprocessorConfig(),
    inference=ParakeetInferenceRunnerConfig(),
    postprocessing=ParakeetPostprocessorConfig(),
)


DocumentSearchQuery = dict[str, Any]
DocId = str


class ASRArgs(DatashareModel):
    project: str
    docs: list[DocId] | DocumentSearchQuery
    config: ASRPipelineConfig = Field(default=_DEFAULT_PIPELINE_CONFIG)
    batch_size: int


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
    def from_asr_handler_result(cls, asr_handler_result: ASRResult) -> Self:
        transcripts = [
            Transcript(text=text, timestamp=Timestamp(start_s=start_s, end_s=end_s))
            for start_s, end_s, text in asr_handler_result.transcription
        ]
        confidence = asr_handler_result.score
        if confidence is not None:
            confidence = math.exp(asr_handler_result.score)
        return Transcription(confidence=confidence, transcripts=transcripts)
