import importlib.metadata
from typing import Annotated

import typer

import datashare_python
from datashare_python.cli.local import local_app
from datashare_python.cli.tasks import task_app
from datashare_python.cli.utils import AsyncTyper

cli_app = AsyncTyper(context_settings={"help_option_names": ["-h", "--help"]})
cli_app.add_typer(task_app)
cli_app.add_typer(local_app)


def version_callback(value: bool) -> None:  # noqa: FBT001
    if value:
        package_version = importlib.metadata.version(datashare_python.__name__)
        print(package_version)
        raise typer.Exit()


@cli_app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
) -> None:
    """Datashare Python CLI."""
    pass
