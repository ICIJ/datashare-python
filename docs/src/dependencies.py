import gc
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

import torch
from datashare_python.exceptions import DependencyInjectionError
from transformers import CohereAsrForConditionalGeneration

ML_MODEL: ContextVar[dict | None] = ContextVar("ml_model")


# --8<-- [start:context_manager]
@contextmanager
def load_ml_model() -> Generator[None, None, None]:
    ml_model = CohereAsrForConditionalGeneration.from_pretrained(
        "CohereLabs/cohere-transcribe-03-2026", device_map="auto"
    )
    ML_MODEL.set(ml_model)
    try:
        yield  # (1)!
    finally:  # (2)!
        del ml_model
        torch.cuda.empty_cache()
        gc.collect()
        ML_MODEL.set(None)


# --8<-- [end:context_manager]


# --8<-- [start:expose_dependency]
def lifespan_ml_model() -> CohereAsrForConditionalGeneration:
    try:
        return ML_MODEL.get()
    except LookupError as e:
        raise DependencyInjectionError("ml model") from e


# --8<-- [end:expose_dependency]

# --8<-- [start:registry]
DEPENDENCIES = {
    "base": [lifespan_ml_model],
}
# --8<-- [end:registry]
