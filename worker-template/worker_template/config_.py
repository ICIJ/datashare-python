from typing import ClassVar

import datashare_python
from datashare_python.config import WorkerConfig
from pydantic import Field

_ALL_LOGGERS = [datashare_python.__name__, __name__, "__main__"]


class TranslateAndClassifyWorkerConfig(WorkerConfig):
    loggers: ClassVar[list[str]] = Field(_ALL_LOGGERS, frozen=True)
