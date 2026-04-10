from typing import Annotated

import datashare_python
import typer
from datashare_python.cli import pretty_exc_callback, version_callback
from datashare_python.cli.utils import AsyncTyper
from icij_common.logging_utils import setup_loggers

import asr_worker

from .models import models_app

cli_app = AsyncTyper(
    context_settings={"help_option_names": ["-h", "--help"]},
    pretty_exceptions_enable=False,
)
cli_app.add_typer(models_app)


@cli_app.callback()
def main(
    version: Annotated[  # noqa: ARG001
        bool | None,
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
    *,
    pretty_exceptions: Annotated[  # noqa: ARG001
        bool,
        typer.Option(
            "--pretty-exceptions", callback=pretty_exc_callback, is_eager=True
        ),
    ] = False,
) -> None:
    """Datashare Python CLI."""
    setup_loggers(["__main__", datashare_python.__name__, asr_worker.__name__])
