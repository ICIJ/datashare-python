from pathlib import Path

import datashare_python
from datashare_python.config import (
    LogFormat,
    LoggingConfig,
    ResourceCacheConfig,
    WorkerConfig,
)
from datashare_python.objects import BaseModel
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


class ASRWorkerConfig(WorkerConfig):
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG

    docs_root: Path = Field(alias="audios_root")
    artifacts_root: Path
    workdir: Path

    # Set max concurrent activity to 1 to avoid parallel inference in practice,
    # this must be set to 1 for the inference worker and > 1 for the io worker
    max_concurrent_activities: int = Field(frozen=True, default=1)

    cache: ASRCache = Field(default_factory=ASRCache)

    def audios_root(self) -> Path:
        return self.docs_root


WORKER_CONFIG_CLS = ASRWorkerConfig
