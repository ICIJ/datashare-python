from pathlib import Path
from typing import ClassVar

import datashare_python
from datashare_python.config import WorkerConfig
from pydantic import Field

_ALL_LOGGERS = [datashare_python.__name__, __name__, "__main__"]


class ASRWorkerConfig(WorkerConfig):
    loggers: ClassVar[list[str]] = Field(_ALL_LOGGERS, frozen=True)

    audios_root: Path
    artifacts_root: Path
    workdir: Path
