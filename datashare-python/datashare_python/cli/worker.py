import asyncio
import logging
from pathlib import Path
from typing import Annotated

import typer
import yaml

from datashare_python.config import WorkerConfig
from datashare_python.constants import DEFAULT_NAMESPACE, DEFAULT_TEMPORAL_ADDRESS
from datashare_python.discovery import discover, discover_activities, discover_workflows
from datashare_python.types_ import TemporalClient
from datashare_python.worker import bootstrap_worker, create_worker_id

from .utils import AsyncTyper

_START_WORKER_HELP = "start a datashare worker"

_LIST_WORKFLOWS_HELP = "list registered workflows"
_LIST_WORKFLOW_NAMES_HELP = "workflow names filters (supports regexes)"

_LIST_ACTIVITIES_HELP = "list registered activities"
_LIST_ACTIVITY_NAMES_HELP = "activity names filters (supports regexes)"

_START_WORKER_WORKFLOWS_HELP = "workflow names run by the worker (supports regexes)"
_START_WORKER_ACTIVITIES_HELP = "activity names run by the worker (supports regexes)"
_START_WORKER_DEPS_HELP = "worker lifetime dependencies name in the registry"
_START_WORKER_WORKER_ID_PREFIX_HELP = "worker ID prefix"
_START_WORKER_CONFIG_PATH_HELP = (
    "path to a worker config YAML file,"
    " if not provided will load worker configuration from env variables"
)
_WORKER_QUEUE_HELP = "worker task queue"
_TEMPORAL_NAMESPACE_HELP = "worker temporal namespace"

_TEMPORAL_URL_HELP = "address for temporal server"
_NAMESPACE_HELP = "namespace name"
_WORKER = "worker"

worker_app = AsyncTyper(name=_WORKER)

logger = logging.getLogger(__name__)


@worker_app.async_command(help=_LIST_WORKFLOWS_HELP)
async def list_workflows(
    names: Annotated[list[str], typer.Argument(help=_LIST_WORKFLOW_NAMES_HELP)],
) -> None:
    workflows = [wf_name for wf_name, _ in discover_workflows(names)]
    if not workflows:
        out = """Couldn't find any registered workflow 🤔.
Make sure your workflow plugins correctly expose workflow entry points, refer to the \
documentation to learn how to do so."""
        print(out)
        return
    workflows = "\n".join(f"- {wf}" for wf in workflows)
    out = f"Found {len(workflows)} registered workflows:\n{workflows}"
    print(out)


@worker_app.async_command(help=_LIST_ACTIVITIES_HELP)
async def list_activities(
    names: Annotated[list[str], typer.Argument(help=_LIST_ACTIVITY_NAMES_HELP)],
) -> None:
    activities = [act_name for act_name, _ in discover_activities(names)]
    if not activities:
        out = """Couldn't find any registered activity 🤔.
    Make sure your activity plugins correctly expose activity entry points, refer \
to the documentation to learn how to do so."""
        print(out)
        return
    activities = "\n".join(f"- {act}" for act in activities)
    out = f"Found {len(activities)} registered activities:\n{activities}"
    print(out)


@worker_app.async_command(help=_START_WORKER_HELP)
async def start(
    workflows: Annotated[list[str], typer.Option(help=_START_WORKER_WORKFLOWS_HELP)],
    activities: Annotated[list[str], typer.Option(help=_START_WORKER_ACTIVITIES_HELP)],
    queue: Annotated[str, typer.Option("--queue", "-q", help=_WORKER_QUEUE_HELP)],
    dependencies: Annotated[
        str | None, typer.Option(help=_START_WORKER_DEPS_HELP)
    ] = None,
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config-path", "--config", "-c", help=_START_WORKER_CONFIG_PATH_HELP
        ),
    ] = None,
    worker_id_prefix: Annotated[
        str | None, typer.Option(help=_START_WORKER_WORKER_ID_PREFIX_HELP)
    ] = None,
    temporal_address: Annotated[
        str, typer.Option("--temporal-address", "-a", help=_TEMPORAL_URL_HELP)
    ] = DEFAULT_TEMPORAL_ADDRESS,
    namespace: Annotated[
        str, typer.Option("--temporal-namespace", "-ns", help=_TEMPORAL_NAMESPACE_HELP)
    ] = DEFAULT_NAMESPACE,
) -> None:
    if config_path is not None:
        with config_path.open() as f:
            bootstrap_config = WorkerConfig.model_validate(
                yaml.load(f, Loader=yaml.Loader)
            )
    else:
        bootstrap_config = WorkerConfig()
    registered_wfs, registered_acts, registered_deps = discover(
        workflows, act_names=activities, deps_name=dependencies
    )
    worker_id = create_worker_id(worker_id_prefix or "worker")
    client = await TemporalClient.connect(temporal_address, namespace=namespace)
    event_loop = asyncio.get_event_loop()
    async with bootstrap_worker(
        worker_id,
        activities=registered_acts,
        workflows=registered_wfs,
        dependencies=registered_deps,
        bootstrap_config=bootstrap_config,
        client=client,
        event_loop=event_loop,
        task_queue=queue,
    ) as worker:
        try:
            await worker.run()
        except Exception as e:  # noqa: BLE001
            await worker.shutdown()
            raise e
