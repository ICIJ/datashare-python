from docling.datamodel.base_models import InputFormat
from docling.datamodel.settings import DebugSettings
from extract_core import (
    BatchConcurrencySettings,
    DoclingPipelineConfig,
    DoclingSettings,
)
from extract_core.docling_ import DoclingFormatOption, InferenceSettings
from extract_worker.config import DoclingWorkerConfig, MarkdownInferenceWorkerConfig
from extract_worker.constants import TorchDevice

_PDF_FORMAT_OPTS_WITH_TIMEOUT = DoclingPipelineConfig().format_options[InputFormat.PDF]
_PDF_FORMAT_OPTS_WITH_TIMEOUT.pipeline_options.update({"document_timeout": 120.0})


def test_default_resolve_pipeline_config_should_override_perf_config() -> None:
    # Given
    worker_config = MarkdownInferenceWorkerConfig()
    pipeline_config = DoclingPipelineConfig(
        settings=DoclingSettings(
            perf=BatchConcurrencySettings(page_batch_size=10, max_page_batches=2),
            debug=DebugSettings(visualize_cells=True),
        )
    )
    # When
    resolved = worker_config.resolve_pipeline_config(pipeline_config)
    # Then
    expected_settings = DoclingSettings(
        perf=BatchConcurrencySettings(page_batch_size=16, max_page_batches=4),
        debug=DebugSettings(visualize_cells=True),
    )
    assert resolved.settings == expected_settings


def test_resolve_pipeline_config_should_set_device() -> None:
    # Given
    worker_config = MarkdownInferenceWorkerConfig(device=TorchDevice.GPU)
    pipeline_config = DoclingPipelineConfig(
        format_options={
            InputFormat.PDF: DoclingFormatOption(
                backend=_PDF_FORMAT_OPTS_WITH_TIMEOUT.backend,
                pipeline_cls=_PDF_FORMAT_OPTS_WITH_TIMEOUT.pipeline_cls,
                pipeline_options={"accelerator_options": "cpu"},
            )
        },
    )
    # When
    resolved = worker_config.resolve_pipeline_config(pipeline_config)
    # Then
    resolved_pdf_opts = resolved.format_options[InputFormat.PDF]
    accelerator_opts = resolved_pdf_opts.pipeline_options["accelerator_options"]
    assert accelerator_opts["device"] == "cuda"


def test_default_resolve_pipeline_config_should_override_document_timeout() -> None:
    # Given
    worker_config = MarkdownInferenceWorkerConfig(
        docling=DoclingWorkerConfig(
            settings=DoclingSettings(
                inference=InferenceSettings(document_timeout=120.0)
            )
        )
    )
    pipeline_config = DoclingPipelineConfig()
    # When
    resolved = worker_config.resolve_pipeline_config(pipeline_config)
    # Then
    expected_settings = DoclingSettings(
        inference=InferenceSettings(document_timeout=120.0)
    )
    assert resolved.settings == expected_settings
    pipeline_opts = resolved.format_options[InputFormat.PDF].pipeline_options
    accelerator_opts = pipeline_opts["document_timeout"]
    assert accelerator_opts == 120.0
