from pathlib import Path

from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.cli import cli_app
from temporalio.worker import Worker
from typer.testing import CliRunner


async def _mock_worker_run(self) -> None:  # noqa: ANN001
    pass


async def test_start_workers(
    typer_asyncio_patch,  # noqa: ANN001, ARG001
    test_worker_config_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    # Given
    config_path = test_worker_config_path
    runner = CliRunner(mix_stderr=False)
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
                "-c",
                str(config_path),
                "--activities",
                "ping",
                "--activities",
                "create-translation-batches",
                "--workflows",
                "ping",
                "--dependencies",
                "base",
                "--temporal-address",
                "localhost:7233",
            ],
            catch_exceptions=False,
        )
    # Then
    assert result.exit_code == 0
    expected = """discovered:
- 1 workflow: ping
- 1 activity: create-translation-batches
- 2 dependencies: set_loggers, set_es_client"""
    assert expected in result.stderr
