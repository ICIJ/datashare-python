# pylint: disable=redefined-outer-name
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import nest_asyncio
import pytest
from icij_worker import Task, TaskError, TaskState
from icij_worker.objects import StacktraceItem
from typer.testing import CliRunner

from datashare_python.cli import cli_app


@pytest.fixture
def typer_asyncio_patch():
    nest_asyncio.apply()


async def test_task_create_task(
    monkeypatch,
    typer_asyncio_patch,  # pylint: disable=unused-argument
):
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client.create_task.return_value = task_id
    mock_client_fn.return_value = mock_client
    monkeypatch.setattr(
        "datashare_python.cli.tasks.DatashareTaskClient", mock_client_fn
    )
    # When
    runner = CliRunner(
        # TODO: for some reason when setting this, the stderr is empty which makes
        #  the test fail. However not using the flag results both output being merged...
        # mix_stderr=False
    )
    result = runner.invoke(cli_app, ["task", "start", "hello_world"])
    # Then
    assert result.exit_code == 0
    assert result.stdout.endswith(task_id + "\n")
    assert "Task(hello_world-some-id) started" in result.stdout
    assert "Task(hello_world-some-id) 🛫" in result.stdout


async def test_task_watch(
    monkeypatch,
    typer_asyncio_patch,  # pylint: disable=unused-argument
):
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client_fn.return_value = mock_client
    created_at = datetime.utcnow()
    mock_client.get_task.side_effect = [
        Task(
            id=task_id,
            name="hello_world",
            progress=0,
            state=TaskState.CREATED,
            created_at=created_at,
        ),
        Task(
            id=task_id,
            name="hello_world",
            progress=0,
            state=TaskState.QUEUED,
            created_at=created_at,
        ),
        Task(
            id=task_id,
            name="hello_world",
            progress=0,
            state=TaskState.RUNNING,
            created_at=created_at,
        ),
        Task(
            id=task_id,
            name="hello_world",
            progress=0.5,
            state=TaskState.RUNNING,
            created_at=created_at,
        ),
        Task(
            id=task_id,
            name="hello_world",
            progress=0.99,
            state=TaskState.RUNNING,
            created_at=created_at,
        ),
        Task(
            id=task_id,
            name="hello_world",
            progress=1.0,
            state=TaskState.DONE,
            completed_at=datetime.utcnow(),
            created_at=created_at,
        ),
    ]
    monkeypatch.setattr(
        "datashare_python.cli.tasks.DatashareTaskClient", mock_client_fn
    )
    # When
    runner = CliRunner(
        # TODO: for some reason when setting this, the stderr is empty which makes
        #  the test fail. However not using the flag results both output being merged...
        # mix_stderr=False
    )
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 0
    assert result.stdout.endswith(task_id + "\n")
    assert "Task(hello_world-some-id) 🛫" in result.stdout
    assert "Task(hello_world-some-id) 🛬" in result.stdout
    assert "Task(hello_world-some-id) ✅" in result.stdout


async def test_task_watch_error(
    monkeypatch,
    typer_asyncio_patch,
    # pylint: disable=unused-argument
):
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client_fn.return_value = mock_client
    created_at = datetime.utcnow()
    mock_client.get_task.return_value = Task(
        id=task_id,
        name="hello_world",
        progress=0,
        state=TaskState.ERROR,
        created_at=created_at,
    )
    mock_client.get_task_error.return_value = TaskError(
        name="SomeError",
        message="some error occurred",
        stacktrace=[StacktraceItem(name="some_func", file="some_file.py", lineno=666)],
    )
    monkeypatch.setattr(
        "datashare_python.cli.tasks.DatashareTaskClient", mock_client_fn
    )
    # When
    runner = CliRunner(
        # TODO: for some reason when setting this, the stderr is empty which makes
        #  the test fail. However not using the flag results both output being merged...
        # mix_stderr=False
    )
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 1
    assert "Task(hello_world-some-id) failed with the following error:" in result.stdout
    assert "Task(hello_world-some-id) ❌" in result.stdout


async def test_task_watch_cancelled(
    monkeypatch,
    typer_asyncio_patch,  # pylint: disable=unused-argument
):
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client_fn.return_value = mock_client
    created_at = datetime.utcnow()
    mock_client.get_task.return_value = Task(
        id=task_id,
        name="hello_world",
        progress=0,
        state=TaskState.CANCELLED,
        created_at=created_at,
    )
    mock_client.get_task_error.return_value = TaskError(
        name="SomeError",
        message="some error occurred",
        stacktrace=[StacktraceItem(name="some_func", file="some_file.py", lineno=666)],
    )
    monkeypatch.setattr(
        "datashare_python.cli.tasks.DatashareTaskClient", mock_client_fn
    )
    # When
    runner = CliRunner(
        # TODO: for some reason when setting this, the stderr is empty which makes
        #  the test fail. However not using the flag results both output being merged...
        # mix_stderr=False
    )
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 1
    assert "Task(hello_world-some-id) was cancelled" in result.stdout
    assert "Task(hello_world-some-id) 🛑" in result.stdout
