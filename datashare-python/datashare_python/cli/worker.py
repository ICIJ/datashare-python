import logging
from typing import Annotated

import typer

from datashare_python.constants import DEFAULT_NAMESPACE, DEFAULT_TEMPORAL_ADDRESS
from datashare_python.discovery import discover_activities, discover_workflows
from datashare_python.types_ import TemporalClient
from datashare_python.worker import datashare_worker

from .utils import AsyncTyper

_START_WORKER_HELP = "start a datashare worker"

_LIST_WORKFLOWS_HELP = "list registered workflows"
_LIST_WORKFLOW_NAMES_HELP = "workflow names filters (supports regexes)"

_LIST_ACTIVITIES_HELP = "list registered activities"
_LIST_ACTIVITY_NAMES_HELP = "activity names filters (supports regexes)"

_START_WORKER_WORKFLOWS_HELP = "workflow names run by the worker (supports regexes)"
_START_WORKER_ACTIVITIES_HELP = "activity names run by the worker (supports regexes)"
_WORKER_QUEUE_HELP = "worker task queue"
_WORKER_MAX_ACTIVITIES_HELP = (
    "maximum number of concurrent activities/tasks"
    " concurrently run by the worker. Defaults to 1 to encourage horizontal scaling."
)
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
    temporal_address: Annotated[
        str, typer.Option("--temporal-address", "-a", help=_TEMPORAL_URL_HELP)
    ] = DEFAULT_TEMPORAL_ADDRESS,
    namespace: Annotated[
        str, typer.Option("--temporal-namespace", "-ns", help=_TEMPORAL_NAMESPACE_HELP)
    ] = DEFAULT_NAMESPACE,
    max_concurrent_activities: Annotated[
        int, typer.Option("--max-activities", help=_WORKER_MAX_ACTIVITIES_HELP)
    ] = 1,
) -> None:
    wf_names, wfs = zip(*discover_workflows(workflows), strict=False)
    registered = ""
    if wf_names:
        n_wfs = len(wf_names)
        registered += (
            f"- {n_wfs} workflow{'s' if n_wfs > 1 else ''}: {','.join(wf_names)}"
        )
    act_names, acts = zip(*discover_activities(activities), strict=False)
    if act_names:
        if registered:
            registered += "\n"
        i = len(act_names)
        registered += f"- {i} activit{'ies' if i > 1 else 'y'}: {','.join(act_names)}"
    if not acts and not wfs:
        raise ValueError("Couldn't find any registered activity or workflow.")
    logger.info("Starting datashare worker running:\n%s", registered)
    client = await TemporalClient.connect(temporal_address, namespace=namespace)
    worker = datashare_worker(
        client,
        workflows=wfs,
        activities=acts,
        task_queue=queue,
        max_concurrent_activities=max_concurrent_activities,
    )
    try:
        await worker.run()
    except Exception as e:  # noqa: BLE001
        await worker.shutdown()
        raise e
