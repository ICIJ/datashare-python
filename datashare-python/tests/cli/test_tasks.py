import logging
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Any, Self
from unittest.mock import AsyncMock, MagicMock

from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.cli import cli_app
from datashare_python.cli import task as cli_task
from datashare_python.objects import StacktraceItem, Task, TaskError, TaskState
from typer.testing import CliRunner


async def test_task_create_task(
    monkeypatch: MonkeyPatch,
    typer_asyncio_patch,  # noqa: ANN001, ARG001
    caplog: LogCaptureFixture,
) -> None:
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client.create_task.return_value = task_id
    mock_client_fn.return_value = mock_client

    class MockedClient:
        def __init__(self, datashare_url: str, api_key: str | None = None) -> None:  # noqa: ARG002
            ...

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001

        async def create_task(
            self,  # noqa: ANN001, ARG001
            name: str,  # noqa: ARG001, ARG002
            args: dict[str, Any],  # noqa: ARG001, ARG002
            *,
            id_: str | None = None,  # noqa: ARG001, ARG002
            group: str | None = None,  # noqa: ARG001, ARG002
        ) -> str:
            return task_id

    monkeypatch.setattr(cli_task, "DatashareTaskClient", MockedClient)
    # When
    runner = CliRunner(catch_exceptions=False)
    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli_app, ["task", "start", "hello_world"])
    # Then
    assert result.exit_code == 0
    assert result.stdout == task_id + "\n"
    assert ("Task(hello_world-some-id) started" in r for r in caplog.records)
    assert ("Task(hello_world-some-id) 🛫" in r for r in caplog.records)


async def test_task_watch(
    monkeypatch: MonkeyPatch,
    typer_asyncio_patch,  # noqa: ANN001, ARG001
) -> None:
    # Given
    task_id = "hello_world-some-id"
    created_at = datetime.now(UTC)

    states = [
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
            completed_at=datetime.now(UTC),
            created_at=created_at,
        ),
    ]

    class MockedClient(AbstractAsyncContextManager):
        def __init__(self, datashare_url: str, api_key: str | None = None) -> None:  # noqa: ARG002
            self._state_it = None

        async def get_task(self, task_id: str) -> Task:  # noqa: ARG002
            return next(self._state_it)

        async def __aenter__(self) -> Self:
            self._state_it = iter(states)
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001

    monkeypatch.setattr(cli_task, "DatashareTaskClient", MockedClient)
    # When
    runner = CliRunner(catch_exceptions=False)
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 0
    assert result.stdout.endswith(task_id + "\n")
    assert "Task(hello_world-some-id) 🛫" in result.stderr
    assert "Task(hello_world-some-id) 🛬" in result.stderr
    assert "Task(hello_world-some-id) ✅" in result.stderr
    assert "1.0" in result.stderr


async def test_task_watch_error(
    monkeypatch: MonkeyPatch,
    typer_asyncio_patch,  # noqa: ANN001, ARG001
) -> None:
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client_fn.return_value = mock_client
    created_at = datetime.now(UTC)

    class MockedClient(AbstractAsyncContextManager):
        def __init__(self, datashare_url: str, api_key: str | None = None) -> None:  # noqa: ARG002
            self._state_it = None

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001

        async def get_task(self, task_id: str) -> Task:  # noqa: ARG002
            return Task(
                id=task_id,
                name="hello_world",
                progress=0,
                state=TaskState.ERROR,
                created_at=created_at,
            )

        async def get_task_error(self, task_id: str) -> TaskError:  # noqa: ARG002
            return TaskError(
                name="SomeError",
                message="some error occurred",
                stacktrace=[
                    StacktraceItem(name="some_func", file="some_file.py", lineno=666)
                ],
            )

    monkeypatch.setattr(cli_task, "DatashareTaskClient", MockedClient)
    # When
    runner = CliRunner(catch_exceptions=False)
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 1
    assert "Task(hello_world-some-id) failed with the following error:" in result.stderr
    assert "Task(hello_world-some-id) ❌" in result.stderr


async def test_task_watch_cancelled(
    monkeypatch: MonkeyPatch,
    typer_asyncio_patch,  # noqa: ANN001, ARG001
) -> None:
    # Given
    mock_client_fn = MagicMock()
    mock_client = AsyncMock()
    task_id = "hello_world-some-id"
    mock_client_fn.return_value = mock_client
    created_at = datetime.now(UTC)

    class MockedClient(AbstractAsyncContextManager):
        def __init__(self, datashare_url: str, api_key: str | None = None) -> None:  # noqa: ARG002
            self._state_it = None

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001

        async def get_task(self, task_id: str) -> Task:  # noqa: ARG002
            return Task(
                id=task_id,
                name="hello_world",
                progress=0,
                state=TaskState.CANCELLED,
                created_at=created_at,
            )

    monkeypatch.setattr(cli_task, "DatashareTaskClient", MockedClient)
    # When
    runner = CliRunner(catch_exceptions=False)
    result = runner.invoke(cli_app, ["task", "watch", task_id, "-p", 0.001])
    # Then
    assert result.exit_code == 1
    assert "Task(hello_world-some-id) was cancelled" in result.stderr
    assert "Task(hello_world-some-id) 🛑" in result.stderr
