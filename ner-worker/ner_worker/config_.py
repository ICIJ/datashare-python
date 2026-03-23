from datetime import timedelta
from typing import ClassVar

import datashare_python
from datashare_python.config import WorkerConfig
from datashare_python.objects import DatashareModel
from pydantic import Field

from .spacy_.ner import SpacyPipelineCache

_ALL_LOGGERS = [datashare_python.__name__, __name__, "__main__"]


class SpacyWorkerConfig(DatashareModel):
    max_spacy_languages_in_memory: int = 1024

    batch_size: int = 1024
    max_length: int = 1024
    max_processes: int = -1
    named_entity_buffer_size: int = 1000
    pipeline_batch_size: int = 1024
    timeout: timedelta = timedelta(hours=1)

    def to_spacy_pipeline_cache(self) -> SpacyPipelineCache:
        return SpacyPipelineCache(self.max_spacy_languages_in_memory)


class NERWorkerConfig(WorkerConfig):
    loggers: ClassVar[list[str]] = Field(_ALL_LOGGERS, frozen=True)

    ne_buffer_size: int = 1000
    spacy: SpacyWorkerConfig = SpacyWorkerConfig()

    def to_spacy_pipeline_cache(self) -> SpacyPipelineCache:
        return self.spacy.to_spacy_pipeline_cache()
