import datashare_python
from datashare_python.config import LoggingConfig, WorkerConfig

_LOGGERS = {datashare_python.__name__: "INFO", __name__: "INFO"}


class TranslateAndClassifyWorkerConfig(WorkerConfig):
    logging: LoggingConfig = LoggingConfig(loggers=_LOGGERS)


WORKER_CONFIG_CLS = TranslateAndClassifyWorkerConfig
