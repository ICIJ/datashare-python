import gc
from typing import TYPE_CHECKING

from caul_core import TorchDevice

if TYPE_CHECKING:
    from types import TracebackType


class TrtInferenceMixin:
    def __init__(self):
        self._encoder = None
        self._decoder = None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ):
        import torch

        self._encoder = None
        self._decoder = None
        if hasattr(self, "_device") and self._device == torch.device(TorchDevice.GPU):
            torch.cuda.empty_cache()
        gc.collect()

        return False
