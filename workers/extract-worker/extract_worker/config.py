from datashare_python.config import WorkerConfig
from datashare_python.objects import DatashareModel
from pydantic import Field

from .constants import TorchDevice


class MarkdownExtractConfig(DatashareModel):
    target_n_pages_per_batch: int = 100


class ExtractWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    markdown: MarkdownExtractConfig = Field(default_factory=MarkdownExtractConfig)


WORKER_CONFIG_CLS = ExtractWorkerConfig
