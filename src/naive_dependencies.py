from contextvars import ContextVar

from transformers import CohereAsrForConditionalGeneration

ML_MODEL: ContextVar[dict | None] = ContextVar("ml_model")  # (1)!


def load_ml_model() -> None:
    ml_model = CohereAsrForConditionalGeneration.from_pretrained(  # (2)!
        "CohereLabs/cohere-transcribe-03-2026", device_map="auto"
    )
    ML_MODEL.set(ml_model)  # (3)!
