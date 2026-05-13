from datashare_python.config import WorkerConfig
from datashare_python.objects import DatashareModel
from pydantic import Field

from .constants import TorchDevice


class C2TranslateConfig(DatashareModel):
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization


class TranslationWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    batch_size: int = 16
    batch_text_length: int = 10000
    batches_per_worker: int = 10
    es_buffer_size: int = 10

    c2_translate: C2TranslateConfig = Field(default_factory=C2TranslateConfig)


WORKER_CONFIG_CLS = TranslationWorkerConfig
