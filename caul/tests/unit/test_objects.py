import pytest
from caul_core import ASRModel


def test_all_asr_models_expose_languages() -> None:
    for model in ASRModel:
        try:
            model.supported_languages()
        except NotImplementedError:
            pytest.fail(f"{model} does not expose supported languages")
