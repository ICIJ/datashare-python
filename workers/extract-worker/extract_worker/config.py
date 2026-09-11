from copy import deepcopy

from datashare_python.config import LoggingConfig, WorkerConfig
from datashare_python.objects import BaseModel, WorkerPaths
from docling.datamodel.base_models import InputFormat
from extract_core import (
    BasePipelineConfig,
    BatchConcurrencySettings,
    DoclingFormatOption,
    DoclingPipelineConfig,
    DoclingSettings,
)
from extract_core.docling_ import InferenceSettings
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
        resolved_settings = self._resolve_docling_settings(pipeline_config)
        format_options = self._resolve_docling_format_options(pipeline_config)
        update = {"settings": resolved_settings, "format_options": format_options}
        resolved = safe_copy(pipeline_config, update=update)
        return resolved

    def _resolve_docling_settings(
        self, pipeline_config: DoclingPipelineConfig
    ) -> BaseModel:
        resolved_perf = pipeline_config.settings.perf.model_dump()
        resolved_perf.update(self.docling.settings.perf.model_dump())
        resolved_perf = BatchConcurrencySettings.model_validate(resolved_perf)
        resolved_inference = pipeline_config.settings.inference.model_dump()
        resolved_inference.update(self.docling.settings.inference.model_dump())
        resolved_inference = InferenceSettings.model_validate(resolved_inference)
        update = {"perf": resolved_perf, "inference": resolved_inference}
        resolved_settings = safe_copy(pipeline_config.settings, update=update)
        return resolved_settings

    def _resolve_docling_format_options(
        self, pipeline_config: DoclingPipelineConfig
    ) -> dict[InputFormat, DoclingFormatOption]:
        resolved = dict()
        doc_timeout = self.docling.settings.inference.document_timeout
        for fmt, opts in pipeline_config.format_options.items():
            pipeline_opts = deepcopy(opts.pipeline_options)
            pipeline_opts.update({"document_timeout": doc_timeout})
            resolved[fmt] = safe_copy(opts, update={"pipeline_options": pipeline_opts})
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
