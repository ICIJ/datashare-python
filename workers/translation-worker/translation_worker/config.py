from datashare_python.config import WorkerConfig
from pydantic import Field

from .constants import TorchDevice


class TranslationWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    batch_size: int = 16
    max_parallel_batches: int = 8
    max_batch_byte_len: int = 1000000
    # ctranslate2 params
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization
