from enum import StrEnum

from datashare_python.objects import DatashareModel
from pydantic import BaseModel, Field

from asr_worker.constants import PARAKEET


class TaskQueues(StrEnum):
    CPU = "asr.transcription.cpu"
    GPU = "asr.transcription.gpu"


class BatchSize(BaseModel):
    """Batch size helper"""

    batch_size: int = 32


class PreprocessingConfig(BatchSize):
    """Preprocessing config"""


class InferenceConfig(BatchSize):
    """Inference config"""

    model_name: str = PARAKEET


class ASRPipelineConfig(BaseModel):
    """ASR pipeline config"""

    preprocessing: PreprocessingConfig = Field(default_factory=PreprocessingConfig)
    inference: InferenceConfig = Field(default_factory=InferenceConfig)


class ASRRequest(DatashareModel):
    file_paths: list[str]
    pipeline: ASRPipelineConfig


class ASRResponse(DatashareModel):
    transcriptions: list[dict] = Field(default_factory=list)
