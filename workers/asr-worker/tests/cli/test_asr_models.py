from asr_worker.cli import cli_app
from asr_worker.objects import AvailableModels
from typer.testing import CliRunner


async def test_list_models(
    typer_asyncio_patch,  # noqa: ANN001, ARG001
) -> None:
    # Given
    runner = CliRunner(mix_stderr=False)
    cmd = ["models", "list-available"]
    # When
    result = runner.invoke(cli_app, cmd, catch_exceptions=False)
    # Then
    assert int(result.exit_code) == 0
    available_models = AvailableModels.model_validate_json(result.output)
    assert isinstance(available_models.root, dict)
