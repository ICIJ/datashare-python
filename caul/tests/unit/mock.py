from typing import ClassVar

import numpy as np
import torch
from caul.tasks import ParakeetInferenceRunner
from caul_core import InferenceRunner, ParakeetInferenceRunnerConfig, TorchDevice
from nemo.collections.asr.parts.mixins import TranscribeConfig
from nemo.collections.asr.parts.utils import Hypothesis
from pydantic import Field

from tests.unit.constant import (
    PARAKEET_TEST_CONFIDENCE,
    PARAKEET_TEST_SEGMENT_END,
    PARAKEET_TEST_SEGMENT_START,
    TEST_WAV_TRANSCRIPTION,
)


class MockNvidiaASRInferenceHandlerResult:
    def __init__(self):
        self.timestep = {
            "segment": [
                {
                    "segment": TEST_WAV_TRANSCRIPTION,
                    "start": PARAKEET_TEST_SEGMENT_START,
                    "end": PARAKEET_TEST_SEGMENT_END,
                }
            ]
        }
        self.text = TEST_WAV_TRANSCRIPTION
        self.score = PARAKEET_TEST_CONFIDENCE


def mock_result() -> Hypothesis:
    timestamp = {
        "segment": [
            {
                "segment": TEST_WAV_TRANSCRIPTION,
                "start": PARAKEET_TEST_SEGMENT_START,
                "end": PARAKEET_TEST_SEGMENT_END,
            }
        ]
    }
    text = TEST_WAV_TRANSCRIPTION
    score = PARAKEET_TEST_CONFIDENCE
    return Hypothesis(timestamp=timestamp, text=text, score=score, y_sequence=[])


class MockParakeetModel:
    # Mirrors the nemo ASRModel.transcribe signature
    def transcribe(
        self,
        audio: list[np.ndarray | torch.Tensor | str]  # noqa: ARG002
        | np.ndarray
        | torch.Tensor
        | str,
        timestamps: bool,  # noqa: ARG002, FBT001
        override_config: TranscribeConfig | None = None,  # noqa: ARG002
    ) -> tuple[list[Hypothesis]]:
        return ([mock_result()],)


class MockNvidiaASRInferenceRunnerConfig(ParakeetInferenceRunnerConfig):
    model: ClassVar[str] = Field(frozen=True, default="mock_nvidia")


@InferenceRunner.register("mock_nvidia")
class MockNvidiaASRInferenceRunner(ParakeetInferenceRunner):
    def __init__(
        self,
        model_name: str,
        device: TorchDevice | torch.device = TorchDevice.CPU,
        batch_size: int = 4,
    ):
        super().__init__(model_name, device, batch_size)

    def __enter__(self):
        self._model = MockParakeetModel()
