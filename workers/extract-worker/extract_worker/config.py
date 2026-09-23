import os
from copy import deepcopy

from datashare_python.config import LoggingConfig, WorkerConfig
from datashare_python.objects import BaseModel, WorkerPaths
from docling.datamodel.accelerator_options import AcceleratorOptions
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
    # We need to expliclity set these so that they are properly merge hence the default
    # values vs. just leaving the default args
    batch_concurrency = BatchConcurrencySettings(
        page_batch_size=_DOCLING_PAGE_BATCH_SIZE,
        max_page_batches=_DOCLING_MAX_PAGE_BATCHES,
    )
    inference = InferenceSettings(document_timeout=None)
    return DoclingSettings(perf=batch_concurrency, inference=inference)


class DoclingWorkerConfig(BaseModel):
    settings: DoclingSettings = Field(default_factory=_default_docling_settings)
    format_options: dict[InputFormat, DoclingFormatOption] = Field(default_factory=dict)


class MarkdownInferenceWorkerConfig(BaseModel):
    default_target_n_pages_per_task: int = 100

    device: TorchDevice = Field(default=TorchDevice.CPU)

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
        # ⚠️ we have to be careful when model_dumping to merge only fields specified by
        # the worker, we use a combination of explicit setting + exclude unset
        # override too much (excluded unset)
        resolved_perf = pipeline_config.settings.perf.model_dump()
        resolved_perf.update(self.docling.settings.perf.model_dump(exclude_unset=True))
        resolved_perf = BatchConcurrencySettings.model_validate(resolved_perf)
        resolved_inference = pipeline_config.settings.inference.model_dump()
        resolved_inference.update(
            self.docling.settings.inference.model_dump(exclude_unset=True)
        )
        resolved_inference = InferenceSettings.model_validate(resolved_inference)
        update = {"perf": resolved_perf, "inference": resolved_inference}
        resolved_settings = safe_copy(pipeline_config.settings, update=update)
        return resolved_settings

    def _resolve_docling_format_options(
        self, pipeline_config: DoclingPipelineConfig
    ) -> dict[InputFormat, DoclingFormatOption]:
        resolved = dict()
        doc_timeout = self.docling.settings.inference.document_timeout
        page_batch_size = self.docling.settings.perf.page_batch_size
        accelerator_opts = AcceleratorOptions(
            num_threads=os.cpu_count(), device=self.device.value
        ).model_dump(exclude_unset=True)
        for fmt, opts in DoclingPipelineConfig().format_options.items():
            if fmt not in pipeline_config.format_options:
                resolved[fmt] = opts
                continue
            pipeline_opts = deepcopy(opts.pipeline_options)
            if pipeline_opts is None:
                pipeline_opts = dict()
            pipeline_opts["document_timeout"] = doc_timeout
            if fmt is InputFormat.PDF:
                pipeline_opts["ocr_batch_size"] = page_batch_size
                pipeline_opts["layout_batch_size"] = page_batch_size
                pipeline_opts["table_batch_size"] = page_batch_size
            update = {"pipeline_options": pipeline_opts}
            if "accelerator_options" not in pipeline_opts:
                pipeline_opts["accelerator_options"] = dict()
            pipeline_opts["accelerator_options"].update(accelerator_opts)
            resolved[fmt] = safe_copy(opts, update=update)
        return resolved


class MarkdownExtractWorkerConfig(BaseModel):
    inference: MarkdownInferenceWorkerConfig = Field(
        default_factory=MarkdownInferenceWorkerConfig
    )


class ExtractWorkerConfig(WorkerConfig):
    logging: LoggingConfig = _DEFAULT_LOGGING_CONFIG

    markdown: MarkdownExtractWorkerConfig = Field(
        default_factory=MarkdownExtractWorkerConfig
    )

    paths: WorkerPaths


WORKER_CONFIG_CLS = ExtractWorkerConfig
