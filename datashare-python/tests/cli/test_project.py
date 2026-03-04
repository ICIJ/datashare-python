from pathlib import Path

from datashare_python.cli import cli_app
from typer.testing import CliRunner


async def test_init_project(
    typer_asyncio_patch,  # noqa: ANN001, ARG001
    local_template_build,  # noqa: ANN001, ARG001
    tmp_path: Path,
) -> None:
    # Given
    runner = CliRunner(mix_stderr=False)
    test_worker_project = "test-project"
    # When
    args = ["project", "init", str(test_worker_project), "-p", str(tmp_path)]
    result = runner.invoke(
        cli_app,
        args,
        catch_exceptions=False,
    )
    # Then
    assert result.exit_code == 0
