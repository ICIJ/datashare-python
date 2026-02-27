import importlib.metadata
import os
from typing import Annotated

import typer
from icij_common.logging_utils import setup_loggers

import datashare_python
from datashare_python.cli.local import local_app
from datashare_python.cli.task import task_app
from datashare_python.cli.utils import AsyncTyper
from datashare_python.cli.worker import worker_app

cli_app = AsyncTyper(context_settings={"help_option_names": ["-h", "--help"]})
cli_app.add_typer(task_app)
cli_app.add_typer(local_app)
cli_app.add_typer(worker_app)


def version_callback(value: bool) -> None:  # noqa: FBT001
    if value:
        package_version = importlib.metadata.version(datashare_python.__name__)
        print(package_version)
        raise typer.Exit()


def pretty_exc_callback(value: bool) -> None:  # noqa: FBT001
    if not value:
        os.environ["TYPER_STANDARD_TRACEBACK"] = "1"


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
    setup_loggers(["__main__", datashare_python.__name__])
