from pathlib import Path

import datashare_python
from datashare_python.config import LoggingConfig, WorkerConfig
from pydantic import Field

_DEFAULT_LOGGERS = {datashare_python.__name__: "INFO", __name__: "INFO"}
_DEFAULT_LOGGING_CONFIG = LoggingConfig(log_in_json=True, loggers=_DEFAULT_LOGGERS)


class ASRWorkerConfig(WorkerConfig):
    logging: LoggingConfig = _DEFAULT_LOGGERS

    docs_root: Path = Field(alias="audios_root")
    artifacts_root: Path
    workdir: Path

    def audios_root(self) -> Path:
        return self.docs_root


WORKER_CONFIG_CLS = ASRWorkerConfig
