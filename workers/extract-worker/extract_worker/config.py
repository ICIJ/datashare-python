from datashare_python.config import LoggingConfig, WorkerConfig
from datashare_python.objects import BaseModel, WorkerPaths
from extract_core import (
    BasePipelineConfig,
    BatchConcurrencySettings,
    DoclingPipelineConfig,
    DoclingSettings,
)
from icij_common.pydantic_utils import safe_copy
from pydantic import Field

from .constants import TorchDevice

loggers = {"datashare_python": "INFO", "extract_python": "INFO", "extract_core": "INFO"}
_DEFAULT_LOGGING_CONFIG = LoggingConfig(loggers=loggers)

_DOCLING_CONVERT_ALL_N_PAGES = 64  # Release the GIL each 64 pages
_DOCLING_PAGE_BATCH_SIZE = 16  # Process 16 pages in ||
_DOCLING_MAX_PAGE_BATCHES = _DOCLING_CONVERT_ALL_N_PAGES // _DOCLING_PAGE_BATCH_SIZE


def _default_docling_settings() -> DoclingSettings:
    batch_concurrency = BatchConcurrencySettings(
        page_batch_size=_DOCLING_MAX_PAGE_BATCHES,
        max_page_batches=_DOCLING_MAX_PAGE_BATCHES,
    )
    return DoclingSettings(perf=batch_concurrency)


class DoclingWorkerConfig(BaseModel):
    settings: DoclingSettings = Field(default_factory=_default_docling_settings)


class MarkdownInferenceWorkerConfig(BaseModel):
    default_target_n_pages_per_task: int = 100

    docling: DoclingWorkerConfig = Field(default_factory=DoclingWorkerConfig)

    def resolve_target_n_pages_per_task(
        self, pipeline_config: BasePipelineConfig
    ) -> int:
        match pipeline_config:
            case DoclingPipelineConfig():
                perf_settings = self.docling.settings.perf
                return (
                    2 * perf_settings.page_batch_size * perf_settings.max_page_batches
                )
        return self.default_target_n_pages_per_task

    def resolve_pipeline_config[C: BasePipelineConfig](
        self, pipeline_config: BasePipelineConfig
    ) -> C:
        match pipeline_config:
            case DoclingPipelineConfig():
                return self._resolve_docling_config(pipeline_config)
        return pipeline_config

    def _resolve_docling_config(
        self, pipeline_config: DoclingPipelineConfig
    ) -> DoclingPipelineConfig:
        resolved_batching = pipeline_config.settings.perf.model_dump()
        resolved_batching.update(self.docling.settings.perf.model_dump())
        resolved_batching = BatchConcurrencySettings.model_validate(resolved_batching)
        resolved_settings = safe_copy(
            pipeline_config.settings, update={"perf": resolved_batching}
        )
        resolved = safe_copy(pipeline_config, update={"settings": resolved_settings})
        return resolved


class MarkdownExtractWorkerConfig(BaseModel):
    inference: MarkdownInferenceWorkerConfig = Field(
        default_factory=MarkdownInferenceWorkerConfig
    )


class ExtractWorkerConfig(WorkerConfig):
    device: TorchDevice = Field(default=TorchDevice.CPU, frozen=True)
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG

    markdown: MarkdownExtractWorkerConfig = Field(
        default_factory=MarkdownExtractWorkerConfig
    )

    paths: WorkerPaths


WORKER_CONFIG_CLS = ExtractWorkerConfig
