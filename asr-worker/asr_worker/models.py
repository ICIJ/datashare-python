from dataclasses import dataclass, field, asdict
from typing import Optional

from asr_worker.constants import PARAKEET


@dataclass
class BatchSize:
    """Batch size helper"""

    batch_size: int = 32


@dataclass
class PreprocessingConfig(BatchSize):
    """Preprocessing config"""


@dataclass
class InferenceConfig(BatchSize):
    """Inference config"""

    model_name: str = PARAKEET


@dataclass
class ASRPipelineConfig:
    """ASR pipeline config"""

    preprocessing: PreprocessingConfig = field(default_factory=PreprocessingConfig)
    inference: InferenceConfig = field(default_factory=InferenceConfig)

    def serialize(self):
        return {
            "preprocessing": asdict(self.preprocessing),
            "inference": asdict(self.inference),
        }


@dataclass
class ASRInputs:
    """Inputs to ASR workflow"""

    file_paths: list[str]
    pipeline: ASRPipelineConfig


@dataclass
class ASRResponse:
    """ASR workflow response"""

    status: str
    transcriptions: list[dict] = field(default_factory=list)
    error: Optional[str] = None
