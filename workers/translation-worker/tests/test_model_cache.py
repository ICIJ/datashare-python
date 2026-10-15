import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from translation_worker import model_cache
from translation_worker.cli import cli_app
from translation_worker.model_cache import cache_models
from translation_worker.objects import DEFAULT_HUNYUAN_MODEL_REF, TranslationModel
from typer.testing import CliRunner


@pytest.mark.parametrize(
    ("model_ref", "expected_repo"),
    [(None, DEFAULT_HUNYUAN_MODEL_REF), ("tencent/HY-MT1.5-7B", "tencent/HY-MT1.5-7B")],
)
def test_cache_models_hunyuan(
    tmp_path: Path, model_ref: str | None, expected_repo: str
) -> None:
    # Given
    hf_hub = MagicMock()
    hf_hub.get_token.return_value = None
    # When
    with patch.dict(sys.modules, {"huggingface_hub": hf_hub}):
        cache_models(TranslationModel.HUNYUAN, tmp_path, model_ref=model_ref)
    # Then
    hf_hub.snapshot_download.assert_called_once_with(
        repo_id=expected_repo, cache_dir=tmp_path, token=None
    )


def test_cache_models_should_raise_for_uncachable_model() -> None:
    with pytest.raises(ValueError, match="can't be cached"):
        cache_models(TranslationModel.ARGOS)


def test_cli_cache_models(tmp_path: Path) -> None:
    # Given
    runner = CliRunner()
    cache = MagicMock()
    cmd = ["model", "cache", "hunyuan", str(tmp_path), "--model-ref", "some/ref"]
    # When
    with patch.object(model_cache, "cache_models", cache):
        result = runner.invoke(cli_app, cmd, catch_exceptions=False)
    # Then
    assert result.exit_code == 0, result.output
    cache.assert_called_once_with(
        TranslationModel.HUNYUAN, tmp_path, model_ref="some/ref"
    )
