from datetime import timedelta

from datashare_python.config import ActivityTimeouts, ResourceCacheConfig, WorkerConfig
from datashare_python.objects import BaseModel, DatashareModel, WorkerPaths
from pydantic import Field

from .objects import TorchDevice


class TranslationCache(BaseModel):
    sentence_splitter: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )
    translator: ResourceCacheConfig = ResourceCacheConfig(
        size=1, exit_context_managers=True
    )


class C2TranslateConfig(DatashareModel):
    beam_size: int = 4
    inter_threads: int = 1
    intra_threads: int = 0
    compute_type: str = "auto"  # quantization


class TranslationTimeouts(BaseModel):
    batching: ActivityTimeouts = ActivityTimeouts(start_to_close=timedelta(minutes=30))
    inference: ActivityTimeouts = ActivityTimeouts(start_to_close=timedelta(hours=1))


class TranslationWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)

    batch_size: int = 16
    batch_text_length: int = 10000
    batches_per_worker: int = 10
    es_buffer_size: int = 10

    timeouts: TranslationTimeouts = Field(default_factory=TranslationTimeouts)
    cache: TranslationCache = Field(default_factory=TranslationCache)

    c2_translate: C2TranslateConfig = Field(default_factory=C2TranslateConfig)

    paths: WorkerPaths


WORKER_CONFIG_CLS = TranslationWorkerConfig
