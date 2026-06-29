import pytest
from constants import TaskQueue
from translation_worker.config import (
    ArgosTranslatorConfig,
    HunyuanMtTranslatorConfig,
    TranslationConfig,
)


@pytest.mark.parametrize(
    ("config", "expected_queue"),
    [
        (
            TranslationConfig(translator=ArgosTranslatorConfig()),
            TaskQueue.C2TRANSLATE_INFERENCE,
        ),
        (
            TranslationConfig(translator=HunyuanMtTranslatorConfig()),
            TaskQueue.TORCH_INFERENCE,
        ),
    ],
)
def test_inference_queue(config: TranslationConfig, expected_queue: TaskQueue) -> None:
    # When
    inference_queue = TaskQueue.inference_queue(config)
    # Then
    assert inference_queue == expected_queue
