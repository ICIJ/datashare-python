from collections.abc import Callable

import pytest
from caul_core import ASRPipeline, ASRPipelineConfig


@pytest.mark.parametrize(
    "model",
    ["parakeet", "fireredasr2"],
)
def test_asr_pipeline_config(model: str) -> None:
    config = getattr(ASRPipelineConfig, model)()
    assert isinstance(config, ASRPipelineConfig)


@pytest.mark.parametrize(
    "pipeline",
    [ASRPipeline.parakeet, ASRPipeline.fireredasr2],
)
def test_should_initialize_asr_pipeline(pipeline: Callable[[], ASRPipeline]) -> None:
    instance = pipeline()
    assert isinstance(instance, ASRPipeline)
