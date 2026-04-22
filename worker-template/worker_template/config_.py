from typing import Annotated

import datashare_python
from datashare_python.config import LoggingConfig, WorkerConfig
from pydantic import Field

_LOGGERS = {datashare_python.__name__: "INFO", __name__: "INFO"}


class TranslateAndClassifyWorkerConfig(WorkerConfig):
    logging: Annotated[LoggingConfig, Field(frozen=True)] = _LOGGERS


WORKER_CONFIG_CLS = TranslateAndClassifyWorkerConfig
