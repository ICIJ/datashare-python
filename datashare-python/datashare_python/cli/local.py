import json
from typing import Annotated

import typer

from datashare_python.cli.utils import AsyncTyper
from datashare_python.constants import DEFAULT_NAMESPACE, DEFAULT_TEMPORAL_ADDRESS
from datashare_python.local_client import LocalClient

_START_WORKERS_HELP = "starts local temporal workers"
_REGISTER_NAMESPACE_HELP = "register namespace"
_WORKERS_HELP = "list of worker paths"
_TEMPORAL_URL_HELP = "address for temporal server"
_NAMESPACE_HELP = "namespace name"
_LOCAL = "local"

local_app = AsyncTyper(name=_LOCAL)


@local_app.async_command(help=_START_WORKERS_HELP)
async def start_workers(
    worker_paths: Annotated[
        str, typer.Option("--worker-paths", "-wp", help=_WORKERS_HELP)
    ] = None,
    temporal_address: Annotated[
        str, typer.Option("--temporal-address", "-a", help=_TEMPORAL_URL_HELP)
    ] = DEFAULT_TEMPORAL_ADDRESS,
) -> None:
    match worker_paths:
        case str():
            worker_paths = json.loads(worker_paths)
        case None:
            worker_paths = dict()
        case _:
            raise TypeError(f"Invalid worker paths: {worker_paths}")

    client = LocalClient()

    await client.start_workers(temporal_address, worker_paths)


@local_app.async_command(help=_REGISTER_NAMESPACE_HELP)
async def register_namespace(
    namespace: Annotated[
        str, typer.Option("--namespace", "-n", help=_NAMESPACE_HELP)
    ] = DEFAULT_NAMESPACE,
    temporal_address: Annotated[
        str, typer.Option("--temporal-address", "-a", help=_TEMPORAL_URL_HELP)
    ] = DEFAULT_TEMPORAL_ADDRESS,
) -> None:
    """Create namespace

    :param namespace: namespace
    :param temporal_address: target host
    """
    client = LocalClient()

    await client.register_namespace(temporal_address, namespace)
