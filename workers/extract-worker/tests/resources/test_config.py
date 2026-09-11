import pytest
from docling.datamodel.settings import DebugSettings
from extract_core import (
    BasePipelineConfig,
    BatchConcurrencySettings,
    DoclingPipelineConfig,
    DoclingSettings,
)
from extract_worker.config import MarkdownInferenceWorkerConfig


@pytest.mark.parametrize(
    ("pipeline_config", "expected_resolved"),
    [
        (
            DoclingPipelineConfig(
                settings=DoclingSettings(
                    perf=BatchConcurrencySettings(
                        page_batch_size=10, max_page_batches=2
                    ),
                    debug=DebugSettings(visualize_cells=True),
                )
            ),
            DoclingPipelineConfig(
                settings=DoclingSettings(
                    perf=BatchConcurrencySettings(
                        page_batch_size=2, max_page_batches=2
                    ),
                    debug=DebugSettings(visualize_cells=True),
                )
            ),
        ),
    ],
)
def test_resolve_pipeline_config(
    pipeline_config: BasePipelineConfig, expected_resolved: BasePipelineConfig
) -> None:
    # Given
    worker_config = MarkdownInferenceWorkerConfig(
        docling=DoclingSettings(
            perf=BatchConcurrencySettings(page_batch_size=2, max_page_batches=2)
        )
    )
    # When
    resolved = worker_config.resolve_pipeline_config(pipeline_config)
    # Then
    assert resolved == expected_resolved
