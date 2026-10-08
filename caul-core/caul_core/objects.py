import datetime
import math
import traceback
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum, unique
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import langcodes
from icij_common.pydantic_utils import (
    icij_config,
    merge_configs,
    no_enum_values_config,
    safe_copy,
)
from pydantic import BaseModel as _BaseModel
from pydantic import Field, GetCoreSchemaHandler, TypeAdapter
from pydantic_core import core_schema
from pydantic_extra_types.language_code import LanguageAlpha2

from .constants import FIREREDASR2_LANGUAGES, PARAKEET_TDT_0_6B_V3_LANGUAGES

if TYPE_CHECKING:
    import torch
    from faster_whisper.transcribe import Segment
    from nemo.collections.asr.parts.utils.rnnt_utils import Hypothesis


# Enums


class FireRedASR2ModelRef(StrEnum):
    ASR2 = "ASR2"


class FireRedASR2ModelTag(StrEnum):
    AED = "aed"
    LLM = "llm"


class VadModelRef(StrEnum):
    SILERO_MODEL = "snakers4/silero-vad"
    PYANNOTE_MODEL = "pyannote/segmentation-3.0"


@unique
class FasterWhisperModel(StrEnum):
    TINY = "tiny"
    BASE = "base"
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    LARGE_V3 = "large-v3"
    DISTIL_LARGE_V3 = "distil-large-v3"

    @property
    def to_repo_id(self) -> str:
        match self:
            case FasterWhisperModel.TINY:
                return "Systran/faster-whisper-tiny"
            case FasterWhisperModel.BASE:
                return "Systran/faster-whisper-base"
            case FasterWhisperModel.SMALL:
                return "Systran/faster-whisper-small"
            case FasterWhisperModel.MEDIUM:
                return "Systran/faster-whisper-medium"
            case FasterWhisperModel.LARGE:
                return "Systran/faster-whisper-large-v3"
            case FasterWhisperModel.LARGE_V3:
                return "Systran/faster-whisper-large-v3"
            case FasterWhisperModel.DISTIL_LARGE_V3:
                return "Systran/faster-distil-whisper-large-v3"
            case _:
                raise NotImplementedError(f"invalid faster whisper model {self}")


# Models


class BaseModel(_BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())


class IETFLanguage(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_plain_validator_function(cls.validate)

    @classmethod
    def validate(cls, v: object) -> Self:
        tag = langcodes.get(str(v))
        if not tag.is_valid():
            raise ValueError(f"Invalid IETF language: {v}")
        return cls(v)


ASRLanguage = IETFLanguage | LanguageAlpha2
_LANGUAGE_TYPE_ADAPTER = TypeAdapter(ASRLanguage)

_VALIDATED_PARAKEET_LANGUAGES = {
    _LANGUAGE_TYPE_ADAPTER.validate_python(lang)
    for lang in PARAKEET_TDT_0_6B_V3_LANGUAGES
}

_VALIDATED_FIREREDASR2_LANGUAGES = {
    _LANGUAGE_TYPE_ADAPTER.validate_python(lang) for lang in FIREREDASR2_LANGUAGES
}


class ASRModel(StrEnum):
    PARAKEET = "parakeet"
    PARAKEET_TRT = "parakeet_trt"
    FASTER_WHISPER = "faster_whisper"
    FIREREDASR2_AED = "fireredasr2_aed"
    WHISPER_TRT = "whisper_trt"
    PREPROCESSING = "preprocessing"

    def supported_languages(self) -> set[ASRLanguage]:
        match self:
            case ASRModel.PARAKEET | ASRModel.PARAKEET_TRT:
                return _VALIDATED_PARAKEET_LANGUAGES
            case ASRModel.FASTER_WHISPER | ASRModel.WHISPER_TRT:
                return set()
            case ASRModel.FIREREDASR2_AED:
                return _VALIDATED_FIREREDASR2_LANGUAGES
            case ASRModel.PREPROCESSING:
                return set()
            case _:
                msg = f"model {self} should expose supported languages"
                raise NotImplementedError(msg)


class BatcherType(StrEnum):
    CONSTANT_SIZE = "constant_size"
    MAX_DURATION = "max_duration"


class SegmentIndex(BaseModel):
    audio: int | str = 0
    segment: int = 0


class ASRResult(BaseModel):
    """Base result class for ASR models"""

    index: SegmentIndex = Field(default_factory=SegmentIndex)
    transcription: list[tuple] = Field(default_factory=list)
    score: float = 1.0

    @property
    def duration(self) -> float:
        if not self.transcription:
            return 0.0
        total = sum(end - start for start, end, _ in self.transcription)
        return total

    @classmethod
    def from_parakeet_hypothesis(cls, hypothesis: "Hypothesis", **extra) -> Self:
        """Parse a hypothesis returned by a Parakeet RNN model

        :param hypothesis: Parakeet hypothesis
        :return: copy of self
        """
        # there's some weird inconsistency here between nemo versions
        timestamps = hypothesis.timestamp
        transcription = [
            (s["start"], s["end"], s["segment"]) for s in timestamps["segment"]
        ]
        return cls(transcription=transcription, score=hypothesis.score, **extra)

    @classmethod
    def from_fireredasr2_result(cls, result: dict, **extra) -> Self:
        """Parse a result dict returned by a FireRedASR2 AED model

        :param result: dict with keys 'text', 'confidence', 'dur_s', 'timestamp'
        :return: ASRResult
        """
        text = result.get("text") or ""
        timestamp = result.get("timestamp") or []
        if timestamp:
            start_s = float(timestamp[0][1])
            end_s = float(timestamp[-1][2])
        else:
            start_s = 0.0
            end_s = float(result.get("dur_s") or 0.0)
        transcription = [(start_s, end_s, text)] if text.strip() else []
        confidence = result.get("confidence")
        score = float(confidence) if confidence is not None else -1.0
        return cls(transcription=transcription, score=score, **extra)

    @classmethod
    def from_faster_whisper_result(cls, segments: "Iterable[Segment]", **extra) -> Self:
        """Parse segments returned by a faster-whisper model

        :param segments: iterable of Segment objects with start, end, text, avg_logprob
        :return: ASRResult
        """
        transcription = []
        weighted_logprob = 0.0
        total_duration = 0.0
        for segment in segments:
            duration = segment.end - segment.start
            transcription.append((segment.start, segment.end, segment.text))
            weighted_logprob += segment.avg_logprob * duration
            total_duration += duration
        score = math.exp(weighted_logprob / total_duration) if total_duration else -1.0
        return cls(transcription=transcription, score=score, **extra)

    def __add__(self, other: Self) -> Self:
        if not isinstance(other, ASRResult):
            msg = f"expected {ASRResult.__class__.__class__} but found: {type(other)}"
            raise TypeError(msg)

        if other.index.audio != self.index.audio:
            raise ValueError("can't merge transcriptions from different audios")

        transcription = self.transcription + other.transcription
        # We have to weight by total segment len
        total_duration = self.duration + other.duration
        score = 1.0
        if total_duration:
            score = self.score * self.duration + other.score * other.duration
            score /= total_duration
        return ASRResult(index=self.index, transcription=transcription, score=score)


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)


def _uuid() -> str:
    return uuid.uuid4().hex


class AudioMetadata(BaseModel):
    index: int | str
    audio_format: str | None = None
    audio_path: Path | None = None


class SegmentMetadata(BaseModel):
    uuid: str = Field(default_factory=_uuid)
    index: SegmentIndex = Field(default_factory=SegmentIndex)
    audio_format: str | None = None
    audio_path: Path | None = None
    duration_s: float
    preprocessed_at: datetime.datetime = Field(default_factory=_utc_now)

    def now(self) -> "SegmentMetadata":
        update = {"preprocessed_at": datetime.datetime.now(datetime.UTC)}
        return safe_copy(self, update=update)


class Error(BaseModel):
    title: str
    detail: str
    metadata: SegmentMetadata | AudioMetadata

    @classmethod
    def from_exception(
        cls, exception: BaseException, metadata: SegmentMetadata | AudioMetadata
    ) -> "Error":
        title = exception.__class__.__name__
        trace_lines = traceback.format_exception(
            None, value=exception, tb=exception.__traceback__
        )
        detail = f"{exception}\n{''.join(trace_lines)}"
        error = Error(title=title, detail=detail, metadata=metadata)
        return error


class FSProcessedSegment(BaseModel):
    metadata: SegmentMetadata
    path: Path


@dataclass(frozen=True)
class MemoryProcessedSegment:
    # Avoid importing torch when importing objects
    metadata: SegmentMetadata
    tensor: "torch.Tensor"


ProcessedAudioSegment = FSProcessedSegment | MemoryProcessedSegment
