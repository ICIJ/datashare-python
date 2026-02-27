from typing import Annotated

import typer

from datashare_python.cli.utils import AsyncTyper
from datashare_python.constants import DEFAULT_NAMESPACE, DEFAULT_TEMPORAL_ADDRESS
from datashare_python.local_client import LocalClient

_REGISTER_NAMESPACE_HELP = "register namespace"
_TEMPORAL_URL_HELP = "address for temporal server"
_NAMESPACE_HELP = "namespace name"
_LOCAL = "local"

local_app = AsyncTyper(name=_LOCAL)


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
