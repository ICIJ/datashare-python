from pathlib import Path

from utils import activity_defn

from .dependencies import lifespan_ml_model


@activity_defn(name="asr-transcription")
def asr_activity(audios: list[Path]) -> list:
    ml_model = lifespan_ml_model()  # (1)!
    return ml_model.transcribe(audios)
