from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar

from caul_core import DEFAULT_SAMPLE_RATE, PARAKEET_INFERENCE_MAX_DURATION_S
from icij_common.registrable import RegistrableConfig
from pydantic import Field

if TYPE_CHECKING:
    import torch


@dataclass(frozen=True)
class TensorSegment:
    """An audio tensor segment with position and duration metadata"""

    tensor: "torch.Tensor"
    segment_start: int
    segment_end: int
    # Unique identifier linking segments to source tensor
    tensor_id: str
    sample_rate: int = DEFAULT_SAMPLE_RATE

    @property
    def duration(self) -> float:
        """Duration of segment in seconds"""
        return self.n_frames / self.sample_rate

    @property
    def n_frames(self) -> int:
        return self.segment_end - self.segment_start


class SegmentationStrategy(StrEnum):
    FIXED = "fixed"
    SILENCE = "silence"
    VOICE_SILERO = "voice_silero"
    VOICE_PYANNOTE = "voice_pyannote"


class SegmentationConfig(RegistrableConfig):
    registry_key: ClassVar[str] = Field(frozen=True, default="strategy")
    strategy: ClassVar[SegmentationStrategy]
    # TODO: parakeet specific variables shouldn't pop in here, rename or define global
    #  naming
    max_segment_len_s: float = PARAKEET_INFERENCE_MAX_DURATION_S


class FixedSegmentationConfig(SegmentationConfig):
    strategy: ClassVar[SegmentationStrategy] = Field(
        frozen=True, default=SegmentationStrategy.FIXED
    )


class SilenceSegmentationConfig(SegmentationConfig):
    """Silence segmentation parameters; see librosa for explanation of parameters"""

    strategy: ClassVar[SegmentationStrategy] = Field(
        frozen=True, default=SegmentationStrategy.SILENCE
    )
    frame_len: int = 2048
    silence_thresh_db: int = 35
    hop_len: int = 512
    kept_silence_len_s: float = 0.15
    min_silence_len_s: float = 0.5


class VoiceSegmentationConfig(SegmentationConfig):
    """Generic VAD config"""

    min_speech_duration_ms: int = 250
    min_silence_duration_ms: int = 100


class SileroVoiceSegmentationConfig(VoiceSegmentationConfig):
    """Voice segmentation parameters using silero VAD;
    see silero for explanation of parameters
    """

    strategy: ClassVar[SegmentationStrategy] = Field(
        frozen=True, default=SegmentationStrategy.VOICE_SILERO
    )
    threshold: float = 0.5
    speech_pad_ms: int = 30


class PyannoteVoiceSegmentationConfig(VoiceSegmentationConfig):
    """Voice segmentation parameters using pyannote VAD;
    see pyannote for explanation of parameters
    """

    strategy: ClassVar[SegmentationStrategy] = Field(
        frozen=True, default=SegmentationStrategy.VOICE_PYANNOTE
    )
