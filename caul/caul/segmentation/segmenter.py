import gc
from collections.abc import Callable
from contextlib import suppress
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self

from caul_core import SILERO_VAD_MODEL, VadModelRef
from icij_common.registrable import RegistrableFromConfig

from .methods import (
    segment_by_pyannote_vad,
    segment_by_silence,
    segment_by_silero_vad,
    segment_fixed,
)
from .objects import (
    PyannoteVoiceSegmentationConfig,
    SegmentationConfig,
    SegmentationStrategy,
    TensorSegment,
)

if TYPE_CHECKING:
    import torch


def _load_vad_model() -> tuple["torch.nn.Module", Callable]:
    """Load silero VAD from torch.hub.

    :return: tuple of silero VAD model with VAD parsing function
    """
    import torch

    model, utils = torch.hub.load(
        VadModelRef.SILERO_MODEL, SILERO_VAD_MODEL, trust_repo=True
    )
    return model, utils[0]


class SegmentationFn(Protocol):
    def __call__(
        self, audio_tensor: "torch.Tensor", **kwargs
    ) -> list[TensorSegment]: ...


class AudioSegmenter(RegistrableFromConfig):
    def __init__(self, config: SegmentationConfig):
        self._config = config
        args = self._config.model_dump()
        args.pop(SegmentationConfig.registry_key.default)
        self._args = args
        self._segmentation_fn: SegmentationFn | None = None

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    @classmethod
    def _from_config(cls, config: SegmentationConfig, **_extras) -> Self:
        return cls(config)

    def segment(self, audio_tensor: "torch.Tensor") -> list[TensorSegment]:
        return self._segmentation_fn(audio_tensor, **self._args)


@AudioSegmenter.register(SegmentationStrategy.FIXED)
class FixedSizeAudioSegmenter(AudioSegmenter):
    def __enter__(self) -> Self:
        self._segmentation_fn = segment_fixed
        return self


@AudioSegmenter.register(SegmentationStrategy.SILENCE)
class SilenceAudioSegmenter(AudioSegmenter):
    def __enter__(self) -> Self:
        self._segmentation_fn = segment_by_silence
        return self


@AudioSegmenter.register(SegmentationStrategy.VOICE_SILERO)
class SileroVoiceAudioSegmenter(AudioSegmenter):
    def __init__(self, config: SegmentationConfig, device: str = "cpu"):
        import torch

        super().__init__(config)
        self._vad_model: torch.nn.Module | None = None
        self._vad_parser_fn: Callable | None = None
        self._device = torch.device(device)

    def __enter__(self) -> Self:
        self._vad_model, self._vad_parser_fn = self._load_vad_model()

        self._segmentation_fn = partial(
            segment_by_silero_vad,
            vad_model=self._vad_model,
            vad_parser_fn=self._vad_parser_fn,
        )
        return self

    def _load_vad_model(self) -> tuple["torch.nn.Module", Callable]:
        model, parser_fn = _load_vad_model()

        return model.to(device=self._device), parser_fn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        import torch

        self._vad_model = None
        self._vad_parser_fn = None
        # Depending on the exact accelerator running might fail to empty the cache
        # but that's OK
        with suppress(RuntimeError):
            torch.accelerator.empty_cache()
        gc.collect()


@AudioSegmenter.register(SegmentationStrategy.VOICE_PYANNOTE)
class PyannoteVoiceAudioSegmenter(AudioSegmenter):
    def __init__(
        self,
        config: PyannoteVoiceSegmentationConfig,
        hf_token: str,
        device: str = "cpu",
    ):
        import torch

        super().__init__(config)
        self._pipeline: Any | None = None
        self._hf_token = hf_token
        self._device = torch.device(device)

    def __enter__(self) -> Self:
        self._pipeline = self._load_pipeline()
        self._segmentation_fn = partial(
            segment_by_pyannote_vad, pipeline=self._pipeline
        )
        return self

    def _load_pipeline(self) -> Any:
        from pyannote.audio import Model
        from pyannote.audio.pipelines import (
            VoiceActivityDetection,
        )

        model = Model.from_pretrained(VadModelRef.PYANNOTE_MODEL, token=self._hf_token)

        return VoiceActivityDetection(segmentation=model).to(device=self._device)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        import torch

        self._pipeline = None
        with suppress(RuntimeError):
            torch.accelerator.empty_cache()
        gc.collect()
