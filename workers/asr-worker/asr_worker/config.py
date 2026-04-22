from pathlib import Path
from typing import Annotated

import datashare_python
from datashare_python.config import LoggingConfig, WorkerConfig
from pydantic import Field

_LOGGERS = {datashare_python.__name__: "INFO", __name__: "INFO"}


class ASRWorkerConfig(WorkerConfig):
    logging: Annotated[LoggingConfig, Field(frozen=True)] = _LOGGERS

    docs_root: Path = Field(alias="audios_root")
    artifacts_root: Path
    workdir: Path

    def audios_root(self) -> Path:
        return self.docs_root


WORKER_CONFIG_CLS = ASRWorkerConfig
