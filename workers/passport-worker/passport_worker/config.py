from concurrent.futures import ProcessPoolExecutor

import datashare_python
from datashare_python.config import (
    LogFormat,
    LoggingConfig,
    ResourceCacheConfig,
    WorkerConfig,
)
from datashare_python.objects import DatashareModel, WorkerPaths
from pydantic import Field

_ALL_LOGGERS = [datashare_python.__name__, __name__, "__main__"]

_DEFAULT_LOGGERS = {
    datashare_python.__name__: "INFO",
    __name__: "INFO",
    "__main__": "INFO",
    "passport_service": "INFO",
}
_DEFAULT_LOGGING_CONFIG = LoggingConfig(
    format=LogFormat.DEFAULT, loggers=_DEFAULT_LOGGERS
)


class ImagePreprocessingWorkerConfig(DatashareModel):
    n_processes: int | None = None
    chunk_size: int = 5

    def to_image_preprocessing_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(max_workers=self.n_processes)


class PDFConversionWorkerConfig(DatashareModel):
    max_concurrency: int = 10


class PreprocessingWorkerConfig(DatashareModel):
    target_n_pages_per_batch: int = 200

    images: ImagePreprocessingWorkerConfig = Field(
        default_factory=ImagePreprocessingWorkerConfig
    )
    pdfs: PDFConversionWorkerConfig = Field(default_factory=PDFConversionWorkerConfig)

    def to_image_preprocessing_executor(self) -> ProcessPoolExecutor:
        return self.images.to_image_preprocessing_executor()


class InferenceWorkerConfig(DatashareModel):
    batch_size: int = 32
    batches_per_task: int = 5


class PreprocessingCacheConfig(DatashareModel):
    pdf: ResourceCacheConfig = Field(default_factory=ResourceCacheConfig)
    images: ResourceCacheConfig = Field(default_factory=ResourceCacheConfig)


class PassportWorkerCacheConfig(DatashareModel):
    preprocessing: PreprocessingCacheConfig = Field(
        default_factory=PreprocessingCacheConfig
    )
    inference: ResourceCacheConfig = Field(default_factory=ResourceCacheConfig)


class PassportWorkerConfig(WorkerConfig):
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG
    paths: WorkerPaths

    cache: PassportWorkerCacheConfig = Field(default_factory=PassportWorkerCacheConfig)

    preprocessing: PreprocessingWorkerConfig = Field(
        default_factory=PreprocessingWorkerConfig
    )
    inference: InferenceWorkerConfig = Field(default_factory=InferenceWorkerConfig)

    def to_image_preprocessing_executor(self) -> ProcessPoolExecutor:
        return self.preprocessing.to_image_preprocessing_executor()


WORKER_CONFIG_CLS = PassportWorkerConfig
