import gc
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING

from icij_common.registrable import FromConfig, RegistrableFromConfig
from overrides import final

from .config import BaseBatcherConfig
from .constants import TorchDevice
from .objects import ASRResult, Error, ProcessedAudioSegment

if TYPE_CHECKING:
    import numpy as np
    import torch


class ASRTask[I, O](AbstractContextManager, ABC):
    """Generic ASR task"""

    def __init__(self, device: TorchDevice = TorchDevice.CPU) -> None:
        self._device = device

    @abstractmethod
    def process(self, inputs: Iterable[I], *args, **kwargs) -> Iterable[O | Error]:
        """Process inputs"""

    @final
    def process_results(
        self, inputs: Iterable[I], *args, skip_errors: bool = False, **kwargs
    ) -> Iterable[O]:
        for r in self.process(inputs, *args, **kwargs):
            if not skip_errors and isinstance(r, Error):
                raise RuntimeError(f"error: {r}")
            yield r

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...

    @property
    def device(self) -> TorchDevice:
        return self._device

    @device.setter
    def device(self, device: TorchDevice) -> None:
        self._device = device


type Audio = "np.ndarray | torch.Tensor | str | Path"
InputItem = Audio  # for backward comp
type AudioID = str
type IdentifiedAudio = tuple[AudioID, Audio]
type ASRInput = "Iterable[Audio | IdentifiedAudio]"
SampleRate = Iterable[int] | int


class Preprocessor(ASRTask, RegistrableFromConfig):
    @abstractmethod
    def process(
        self,
        inputs: ASRInput,
        sample_rates: SampleRate | None = None,
        output_dir: Path | None = None,
        **kwargs,
    ) -> Iterable[tuple[ProcessedAudioSegment, ...] | Error]:
        """Process inputs"""


class InferenceRunner(ASRTask, RegistrableFromConfig):
    """Abstract for ASR inference"""

    def __init__(self, device: TorchDevice = TorchDevice.CPU):
        self._device = device

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        import torch

        self._model = None
        gc.collect()
        if self._torch_device.type == torch.device(TorchDevice.GPU).type:
            torch.cuda.empty_cache()

    @abstractmethod
    def process(
        self, inputs: Iterable[tuple[ProcessedAudioSegment, ...]], *args, **kwargs
    ) -> Iterable[ASRResult | Error]: ...

    @property
    def _torch_device(self) -> "torch.device":
        import torch

        return torch.device(self.device)

    @property
    def device(self) -> TorchDevice:
        return self._device

    @device.setter
    def device(self, device: TorchDevice) -> None:
        self._device = device


class Postprocessor(ASRTask, RegistrableFromConfig):
    def process(
        self, inputs: Iterable[ASRResult], *args, **kwargs
    ) -> Iterable[ASRResult | Error]: ...


class Batcher[I, O, C](RegistrableFromConfig):
    def __init__(self, items: Iterable[I | Error], config: C):
        self._items = items
        self._config = config

    @abstractmethod
    def batch(self) -> Iterable[tuple[O, ...] | Error]: ...

    @classmethod
    def _from_config(
        cls, config: BaseBatcherConfig, *, items: Iterable[I | Error]
    ) -> FromConfig:
        return cls(config=config, items=items)
