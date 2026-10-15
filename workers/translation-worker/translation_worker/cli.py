import logging
from pathlib import Path
from typing import Annotated

import typer
from icij_common.logging_utils import setup_loggers

import translation_worker

from . import model_cache
from .objects import TranslationModel

_CACHE_MODELS_HELP = "cache translation models in the provided location"
_CACHE_MODELS_MODEL_HELP = "translation model to cache"
_CACHE_MODELS_CACHE_DIR_HELP = (
    "cache directory, if not provided defaults to HF cache directory"
)
_CACHE_MODELS_MODEL_REF_HELP = "model HF repo, defaults to the model default"

logger = logging.getLogger(__name__)

cli_app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    pretty_exceptions_enable=False,
)
models_app = typer.Typer(name="model")
cli_app.add_typer(models_app)


@cli_app.callback()
def main() -> None:
    setup_loggers(["__main__", translation_worker.__name__])


@models_app.command(help=_CACHE_MODELS_HELP)
def cache(
    model: Annotated[
        TranslationModel,
        typer.Argument(help=_CACHE_MODELS_MODEL_HELP, case_sensitive=False),
    ],
    cache_dir: Annotated[
        Path | None, typer.Argument(help=_CACHE_MODELS_CACHE_DIR_HELP)
    ] = None,
    model_ref: Annotated[
        str | None, typer.Option(help=_CACHE_MODELS_MODEL_REF_HELP)
    ] = None,
) -> None:
    model_cache.cache_models(model, cache_dir, model_ref=model_ref)
