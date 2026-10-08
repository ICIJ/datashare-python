import pytest
from caul.tasks import (
    FasterWhisperInferenceRunner,
    FireRedASR2InferenceRunner,
    FireRedASR2Postprocessor,
    FireRedASR2Preprocessor,
    ParakeetInferenceRunner,
    ParakeetPostprocessor,
    ParakeetPreprocessor,
)
from caul_core import (
    ASRPipeline,
    ASRPipelineConfig,
    ParakeetInferenceRunnerConfig,
    ParakeetPostprocessorConfig,
    ParakeetPreprocessorConfig,
)

_PARAKEET = """
{
    "preprocessing": {
        "model": "parakeet"
    },
    "inference": {
        "model": "parakeet"
    },
    "postprocessing": {
        "model": "parakeet"
    }
}
"""

_FIREREDASR2 = """
{
    "preprocessing": {
        "model": "fireredasr2_aed"
    },
    "inference": {
        "model": "fireredasr2_aed"
    },
    "postprocessing": {
        "model": "fireredasr2_aed"
    }
}
"""

_PARAKEET_WITH_WHISPER_INFERENCE = """
{
    "preprocessing": {
        "model": "parakeet"
    },
    "inference": {
        "model": "faster_whisper"
    },
    "postprocessing": {
        "model": "parakeet"
    }
}
"""


@pytest.mark.parametrize(
    ("pipeline_config_json", "expected_types"),
    [
        (
            _PARAKEET,
            [ParakeetPreprocessor, ParakeetInferenceRunner, ParakeetPostprocessor],
        ),
        (
            _PARAKEET_WITH_WHISPER_INFERENCE,
            [ParakeetPreprocessor, FasterWhisperInferenceRunner, ParakeetPostprocessor],
        ),
        (
            _FIREREDASR2,
            [
                FireRedASR2Preprocessor,
                FireRedASR2InferenceRunner,
                FireRedASR2Postprocessor,
            ],
        ),
    ],
)
def test_asr_pipeline_from_config(
    pipeline_config_json: str, expected_types: list[type]
) -> None:
    # Given
    pipeline_config = ASRPipelineConfig.model_validate_json(pipeline_config_json)
    # When
    pipeline = ASRPipeline.from_config(pipeline_config)

    # Then
    for component, expected_type in zip(pipeline.tasks, expected_types, strict=True):
        assert isinstance(component, expected_type)


def test_config_serde() -> None:
    # Given
    config = ASRPipelineConfig(
        preprocessing=ParakeetPreprocessorConfig(),
        inference=ParakeetInferenceRunnerConfig(),
        postprocessing=ParakeetPostprocessorConfig(),
    )
    # When
    ser = config.model_dump_json(indent=2)
    deser = ASRPipelineConfig.model_validate_json(ser)
    # Then
    assert deser == config
