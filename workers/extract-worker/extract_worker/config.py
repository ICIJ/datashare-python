from datashare_python.config import LoggingConfig, WorkerConfig
from datashare_python.objects import DatashareModel
from pydantic import Field

from .constants import TorchDevice

loggers = {"datashare_python": "INFO", "extract_python": "INFO", "extract_core": "INFO"}
_DEFAULT_LOGGING_CONFIG = LoggingConfig(loggers=loggers)


class MarkdownExtractConfig(DatashareModel):
    target_n_pages_per_batch: int = 100


class ExtractWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG

    markdown: MarkdownExtractConfig = Field(default_factory=MarkdownExtractConfig)


WORKER_CONFIG_CLS = ExtractWorkerConfig
