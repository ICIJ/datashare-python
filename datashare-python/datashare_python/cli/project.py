import logging
from pathlib import Path
from typing import Annotated

import typer

from datashare_python.template import init_project

from .utils import AsyncTyper, eprint

_INIT_PROJECT_HELP = "initialize a new worker project from a template"
_INIT_PROJECT_NAME_HELP = "name of the new worker package"
_INIT_PROJECT_PATH_HELP = "path where project will be created"

_PROJECT = "project"

project_app = AsyncTyper(name=_PROJECT)

logger = logging.getLogger(__name__)


@project_app.async_command(help=_INIT_PROJECT_HELP)
async def init(
    name: Annotated[str, typer.Argument(help=_INIT_PROJECT_NAME_HELP)],
    path: Annotated[
        Path | None, typer.Option("--path", "-p", help=_INIT_PROJECT_PATH_HELP)
    ] = None,
) -> None:
    if path is None:
        path = Path(".")
    eprint(f"Initializing {name} worker project in {path.absolute()}...")
    init_project(name, path)
    eprint(f"Project {name} initialized !")
    print(path)
