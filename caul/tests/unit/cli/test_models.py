import logging
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from caul.cli import cli_app, models
from caul_core import ASRModel
from typer.testing import CliRunner

logger = logging.getLogger(__name__)


def _mock_cache_models(model: ASRModel | None, cache_dir: Path) -> None:
    logger.info("%s models were cached %s", model, cache_dir)


@pytest.mark.asyncio
@pytest.mark.usefixtures("typer_asyncio_patch")
async def test_cache_models(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    # Given
    runner = CliRunner(catch_exceptions=False)
    monkeypatch.setattr(models, "cache_models", _mock_cache_models)
    # When
    cmd = ["model", "cache", "parakeet"]
    with caplog.at_level(logging.INFO, logger=logger.name):
        result = runner.invoke(cli_app, cmd, catch_exceptions=False)
    # Then
    assert result.exit_code == 0
    expected = "parakeet models were cached"
    assert expected in caplog.text
