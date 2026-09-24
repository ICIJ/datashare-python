from datetime import timedelta

import datashare_python
from caul_core import TorchDevice
from datashare_python.config import (
    ActivityTimeouts,
    LogFormat,
    LoggingConfig,
    ResourceCacheConfig,
    WorkerConfig,
)
from datashare_python.objects import BaseModel, WorkerPaths
from pydantic import Field

_DEFAULT_LOGGERS = {datashare_python.__name__: "INFO", __name__: "INFO"}
_DEFAULT_LOGGING_CONFIG = LoggingConfig(
    format=LogFormat.DEFAULT, loggers=_DEFAULT_LOGGERS
)


class ASRCache(BaseModel):
    preprocessor: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )
    inference_runner: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )
    postprocessor: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )


class IndexingWorkerConfig(BaseModel):
    target_bulk_char_size: int = 50_000


class ASRWorkerDevices(BaseModel):
    inference: TorchDevice = TorchDevice.CPU


class ASRTimeouts(BaseModel):
    search: ActivityTimeouts = ActivityTimeouts(start_to_close=timedelta(minutes=10))
    preprocessing: ActivityTimeouts = ActivityTimeouts(
        start_to_close=timedelta(minutes=30)
    )
    inference: ActivityTimeouts = ActivityTimeouts(
        start_to_close=timedelta(hours=1), heartbeat=timedelta(minutes=3)
    )
    postprocessing: ActivityTimeouts = ActivityTimeouts(
        start_to_close=timedelta(minutes=10)
    )
    indexing: ActivityTimeouts = ActivityTimeouts(start_to_close=timedelta(minutes=15))
    aggregation: ActivityTimeouts = ActivityTimeouts(
        start_to_close=timedelta(minutes=5)
    )


class ASRWorkerConfig(WorkerConfig):
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG

    paths: WorkerPaths

    indexing: IndexingWorkerConfig = Field(default_factory=IndexingWorkerConfig)

    # Set max concurrent activity to 1 to avoid parallel inference in practice,
    # this must be set to 1 for the inference worker and > 1 for the io worker
    max_concurrent_activities: int = Field(frozen=True, default=1)

    cache: ASRCache = Field(default_factory=ASRCache)

    devices: ASRWorkerDevices = Field(default_factory=ASRWorkerDevices)

    timeouts: ASRTimeouts = Field(default_factory=ASRTimeouts)


WORKER_CONFIG_CLS = ASRWorkerConfig
