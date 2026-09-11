import pytest
from docling.datamodel.base_models import InputFormat
from extract_core import (
    BasePipelineConfig,
    BatchConcurrencySettings,
    DoclingPipelineConfig,
    DoclingSettings,
)
from extract_core.docling_ import InferenceSettings
from extract_worker.config import DoclingWorkerConfig, MarkdownInferenceWorkerConfig

_PDF_FORMAT_OPTS_WITH_TIMEOUT = DoclingPipelineConfig().format_options[InputFormat.PDF]
_PDF_FORMAT_OPTS_WITH_TIMEOUT.pipeline_options.update({"document_timeout": 120.0})


@pytest.mark.parametrize(
    ("worker_config", "pipeline_config", "expected_resolved"),
    [
        # (
        #     MarkdownInferenceWorkerConfig(
        #         docling=DoclingWorkerConfig(
        #             settings=DoclingSettings(
        #                 perf=BatchConcurrencySettings(
        #                     page_batch_size=2, max_page_batches=2
        #                 )
        #             )
        #         )
        #     ),
        #     DoclingPipelineConfig(
        #         settings=DoclingSettings(
        #             perf=BatchConcurrencySettings(
        #                 page_batch_size=10, max_page_batches=2
        #             ),
        #             debug=DebugSettings(visualize_cells=True),
        #         )
        #     ),
        #     DoclingPipelineConfig(
        #         settings=DoclingSettings(
        #             perf=BatchConcurrencySettings(
        #                 page_batch_size=2, max_page_batches=2
        #             ),
        #             debug=DebugSettings(visualize_cells=True),
        #         )
        #     ),
        # ),
        (
            MarkdownInferenceWorkerConfig(
                docling=DoclingWorkerConfig(
                    settings=DoclingSettings(
                        inference=InferenceSettings(document_timeout=120.0)
                    )
                )
            ),
            DoclingPipelineConfig(
                settings=DoclingSettings(
                    perf=BatchConcurrencySettings(),
                    inference=InferenceSettings(document_timeout=10.0),
                )
            ),
            DoclingPipelineConfig(
                format_options={InputFormat.PDF: _PDF_FORMAT_OPTS_WITH_TIMEOUT},
                settings=DoclingSettings(
                    perf=BatchConcurrencySettings(),
                    inference=InferenceSettings(document_timeout=120.0),
                ),
            ),
        ),
    ],
)
def test_resolve_pipeline_config(
    worker_config: MarkdownInferenceWorkerConfig,
    pipeline_config: BasePipelineConfig,
    expected_resolved: BasePipelineConfig,
) -> None:
    # When
    resolved = worker_config.resolve_pipeline_config(pipeline_config)
    # Then
    # dump for nicer display in case of failure
    assert resolved.model_dump() == expected_resolved.model_dump()
