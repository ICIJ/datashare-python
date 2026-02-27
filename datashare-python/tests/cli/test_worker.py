from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.cli import cli_app
from temporalio.worker import Worker
from typer.testing import CliRunner


async def _mock_worker_run(self) -> None:  # noqa: ANN001
    pass


async def test_start_workers(
    worker_lifetime_deps,  # noqa: ANN001, ARG001
    typer_asyncio_patch,  # noqa: ANN001, ARG001
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    # Given
    runner = CliRunner(catch_exceptions=False)
    monkeypatch.setattr(Worker, "run", _mock_worker_run)
    with capsys.disabled():
        # When
        result = runner.invoke(
            cli_app,
            [
                "worker",
                "start",
                "--queue",
                "cpu",
                "--activities",
                "ping",
                "--activities",
                "create-translation-batches",
                "--workflows",
                "ping",
                "--temporal-address",
                "localhost:7233",
            ],
        )
    # Then
    assert result.exit_code == 0
    expected = """Starting datashare worker running:
- 1 workflow: ping
- 1 activity: create-translation-batches"""
    assert expected in result.stderr
