from pathlib import Path

from transformers import CohereAsrForConditionalGeneration
from utils import activity_defn


@activity_defn(name="asr-transcription")
def asr_activity(audios: list[Path]) -> list:
    ml_model = CohereAsrForConditionalGeneration.from_pretrained(  # (1)!
        "CohereLabs/cohere-transcribe-03-2026", device_map="auto"
    )
    return ml_model.transcribe(audios)
