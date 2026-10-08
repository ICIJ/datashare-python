import importlib.metadata
import os
from typing import Annotated

import typer
from icij_common.logging_utils import setup_loggers

import caul

from .models import models_app
from .utils import AsyncTyper

cli_app = AsyncTyper(
    context_settings={"help_option_names": ["-h", "--help"]},
    pretty_exceptions_enable=False,
)
cli_app.add_typer(models_app)


def version_callback(value: bool) -> None:  # noqa: FBT001 typer callback signature
    if value:
        package_version = importlib.metadata.version(caul.__name__)
        print(package_version)
        raise typer.Exit()


def pretty_exc_callback(value: bool) -> None:  # noqa: FBT001 typer callback signature
    if not value:
        os.environ["TYPER_STANDARD_TRACEBACK"] = "1"


@cli_app.callback()
def main(
    version: Annotated[  # noqa: ARG001 handled by its eager typer callback
        bool | None,
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
    *,
    pretty_exceptions: Annotated[  # noqa: ARG001 handled by its eager typer callback
        bool,
        typer.Option(
            "--pretty-exceptions", callback=pretty_exc_callback, is_eager=True
        ),
    ] = False,
) -> None:
    """Datashare Python CLI."""
    setup_loggers(["__main__", caul.__name__])
