import asyncio
import json
import logging
import sys
from pathlib import Path
from traceback import FrameSummary, StackSummary
from typing import Annotated, Any, Optional

import typer
from alive_progress import alive_bar
from icij_worker import TaskState
from icij_worker.objects import READY_STATES, Task, TaskError

from datashare_python.cli.utils import AsyncTyper, eprint
from datashare_python.constants import PYTHON_TASK_GROUP
from datashare_python.task_client import DatashareTaskClient

logger = logging.getLogger(__name__)

DEFAULT_DS_ADDRESS = "http://localhost:8080"

_ARGS_HELP = "task argument as a JSON string or file path"
_GROUP_HELP = "task group"
_DS_API_KEY_HELP = "datashare API key"
_DS_URL_HELP = "datashare address"
_POLLING_INTERVAL_S_HELP = "task state polling interval in seconds"
_NAME_HELP = "registered task name"
_RESULT_HELP = "get a task result"
_START_HELP = "creates a new task and start it"
_TASK_ID_HELP = "task ID"
_WATCH_HELP = "watch a task until it's complete"

TaskArgs = str

task_app = AsyncTyper(name="task")


@task_app.async_command(help=_START_HELP)
async def start_task(
    name: Annotated[str, typer.Argument(help=_NAME_HELP)],
    args: Annotated[TaskArgs, typer.Argument(help=_ARGS_HELP)] = None,
    group: Annotated[
        Optional[str],
        typer.Option("--group", "-g", help=_GROUP_HELP),
    ] = PYTHON_TASK_GROUP.name,
    ds_address: Annotated[
        str, typer.Option("--ds-address", "-a", help=_DS_URL_HELP)
    ] = DEFAULT_DS_ADDRESS,
    ds_api_key: Annotated[
        Optional[str], typer.Option("--ds-api-key", "-k", help=_DS_API_KEY_HELP)
    ] = None,
):
    match args:
        case str():
            as_path = Path(name)
            if as_path.exists():
                args = json.loads(as_path.read_text())
            else:
                args = json.loads(args)
        case None:
            args = dict()
        case _:
            raise TypeError(f"Invalid args {args}")
    client = DatashareTaskClient(ds_address, api_key=ds_api_key)
    async with client:
        task_id = await client.create_task(name, args, group=group)
    eprint(f"Task({task_id}) started !")
    eprint(f"Task({task_id}) 🛫")
    print(task_id)


@task_app.async_command(help=_WATCH_HELP)
async def watch(
    task_id: Annotated[str, typer.Argument(help=_TASK_ID_HELP)],
    ds_address: Annotated[
        str, typer.Option("--ds-address", "-a", help=_DS_URL_HELP)
    ] = DEFAULT_DS_ADDRESS,
    ds_api_key: Annotated[
        Optional[str], typer.Option("--ds-api-key", "-k", help=_DS_API_KEY_HELP)
    ] = None,
    polling_interval_s: Annotated[
        float, typer.Option("--polling-interval-s", "-p", help=_POLLING_INTERVAL_S_HELP)
    ] = 1.0,
):
    client = DatashareTaskClient(ds_address, api_key=ds_api_key)
    async with client:
        task = await client.get_task(task_id)
        if task.state is READY_STATES:
            await _handle_ready(task, client, already_done=True)
        await _handle_alive(task, client, polling_interval_s)
    print(task_id)


@task_app.async_command(help=_RESULT_HELP)
async def result(
    task_id: Annotated[str, typer.Argument(help=_TASK_ID_HELP)],
    ds_address: Annotated[
        str, typer.Option("--ds-address", "-a", help=_DS_URL_HELP)
    ] = DEFAULT_DS_ADDRESS,
    ds_api_key: Annotated[
        Optional[str], typer.Option("--ds-api-key", "-k", help=_DS_API_KEY_HELP)
    ] = None,
) -> Any:
    client = DatashareTaskClient(ds_address, api_key=ds_api_key)
    async with client:
        res = await client.get_task_result(task_id)
        if isinstance(res, (dict, list)):
            res = json.dumps(res, indent=2)
        print(res)


async def _handle_ready(
    task: Task, client: DatashareTaskClient, already_done: bool = False
) -> None:
    match task.state:
        case TaskState.ERROR:
            await _handle_error(task, client)
        case TaskState.CANCELLED:
            await _handle_cancelled(task)
        case TaskState.DONE:
            if already_done:
                await _handle_already_done(task)
            else:
                await _handle_done(task)
        case _:
            raise ValueError(f"Unexpected task state {task.state}")


async def _handle_error(task, client: DatashareTaskClient):
    error = await client.get_task_error(task.id)
    eprint(
        f"Task({task.id}) failed with the following"
        f" error:\n\n{_format_error(error)}"
    )
    eprint(f"Task({task.id}) ❌")
    raise typer.Exit(code=1)


async def _handle_cancelled(task):
    eprint(f"Task({task.id}) was cancelled !")
    eprint(f"Task({task.id}) 🛑")
    raise typer.Exit(code=1)


async def _handle_already_done(task):
    eprint(f"Task({task.id}) ✅ is already completed !")


async def _handle_done(task):
    eprint(f"Task({task.id}) 🛬")
    eprint(f"Task({task.id}) ✅")


async def _handle_alive(
    task: Task, client: DatashareTaskClient, polling_interval_s: float
) -> None:
    title = f"Task({task.id}) 🛫"
    stats = "(ETA: {eta})"
    monitor = "{percent}"
    progress_bar = alive_bar(
        title=title, manual=True, stats=stats, monitor=monitor, file=sys.stderr
    )
    with progress_bar as bar:
        task_state = task.state
        while task_state not in READY_STATES:
            task = await client.get_task(task.id)
            task_state = task.state
            progress = task.progress or 0.0
            bar(progress)  # pylint: disable=not-callable
            await asyncio.sleep(polling_interval_s)
    if task_state in READY_STATES:
        await _handle_ready(task, client)


def _format_error(error: TaskError) -> str:
    stack = StackSummary.from_list(
        [FrameSummary(f.name, f.lineno, f.name) for f in error.stacktrace]
    )
    msg = f"{error.name}:\n{stack}\n{error.message}"
    if error.cause:
        msg += "\n cause by {error.cause}"
    return msg
