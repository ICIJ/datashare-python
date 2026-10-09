import logging
from pathlib import Path
from typing import Annotated

import typer
from caul_core import ASRModel

from caul.model_cache import cache_models

from .utils import AsyncTyper

_START_WORKER_HELP = "start a datashare worker"

_CACHE_MODELS_HELP = "cache ASR models to the provided location"
_CACHE_MODELS_NAMES_HELP = (
    "ASR model for which we should cache models, if not provided cache all models"
)
_CACHE_MODELS_CACHE_DIR_HELP = (
    "cache directory, if not provided defaults to HuggingFace cache directory"
)

_MODEL = "model"

models_app = AsyncTyper(name=_MODEL)

logger = logging.getLogger(__name__)


@models_app.async_command(help=_CACHE_MODELS_HELP)
async def cache(
    model: Annotated[ASRModel, typer.Argument(help=_CACHE_MODELS_NAMES_HELP)] = None,
    cache_dir: Annotated[
        Path | None, typer.Argument(help=_CACHE_MODELS_CACHE_DIR_HELP)
    ] = None,
) -> None:
    from huggingface_hub.constants import (
        HF_HUB_CACHE,
    )

    if cache_dir is None:
        cache_dir = HF_HUB_CACHE
    cache_models(model, cache_dir)
