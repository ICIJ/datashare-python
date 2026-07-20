import math
from collections import defaultdict
from functools import cache
from typing import Any, ClassVar, Self

from caul_core import ASRPipelineConfig, ASRResult
from caul_core.objects import ASRLanguage, ASRModel
from datashare_python.objects import (
    ArtifactType,
    DatashareModel,
    DocArtifact,
    ManifestEntry,
    TaskArgs,
)
from pydantic import Field, RootModel

DocumentSearchQuery = dict[str, Any]
DocId = str


class TranscriptionManifestEntry(ManifestEntry):
    confidence: float | None


class TranscriptionArtifact(DocArtifact):
    filename: ClassVar[str] = "transcription.json"
    type: ClassVar[ArtifactType] = ArtifactType.ASR_TRANSCRIPTION


class ASRIndexingConfig(DatashareModel):
    transcript_sep: str = "\n"
    speaker_sep: str = "\n\n"


class ASRArgs(TaskArgs):
    project: str
    docs: list[DocId] | DocumentSearchQuery
    config: ASRPipelineConfig = Field(default_factory=ASRPipelineConfig.parakeet)
    batch_size: int
    indexing: ASRIndexingConfig = Field(default_factory=ASRIndexingConfig)

    def as_manifest_task_input(self) -> dict[str, Any]:
        as_entry = super().as_manifest_task_input()
        as_entry.pop("docs")
        return as_entry


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

    def as_text(self, transcript_sep: str = "\n", *, speaker_sep: str = "\n\n") -> str:
        current_speaker = None
        current_speaker_texts = []
        texts = []
        for t in self.transcripts:
            new_speaker = t.speaker
            if new_speaker != current_speaker and current_speaker_texts:
                texts.append(transcript_sep.join(current_speaker_texts))
                current_speaker_texts = []
            current_speaker_texts.append(t.text)
            current_speaker = new_speaker
        if current_speaker_texts:
            texts.append(transcript_sep.join(current_speaker_texts))
        return speaker_sep.join(texts)


AvailableModels = RootModel[dict[ASRLanguage, list[ASRModel]]]


@cache
def available_models() -> AvailableModels:
    models = defaultdict(list)
    for m in ASRModel:
        for language in m.supported_languages():
            models[language].append(m)
    models = {k: sorted(v) for k, v in sorted(models.items())}
    return AvailableModels.model_validate(models)
