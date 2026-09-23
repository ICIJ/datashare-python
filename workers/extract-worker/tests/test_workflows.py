import uuid

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
)
from extract_core import DoclingPipelineConfig
from extract_worker.objects import MarkdownExtractArgs, MarkdownExtractResponse
from extract_worker.workflows import ExtractMarkdownContentWorkflow, TaskQueues
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker


@pytest.mark.e2e
async def test_extract_markdown_workflow_e2e(
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    md_extract_cpu_worker: Worker,  # noqa: ARG001
    test_temporal_client: TemporalClient,
    docs_with_cached_artifacts: list[ProcessedFile],
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-markdown-{uuid.uuid4()}"
    doc_ids = [d.doc_id for d in docs_with_cached_artifacts]
    args = MarkdownExtractArgs(
        project=TEST_PROJECT, docs=doc_ids, config=DoclingPipelineConfig()
    )

    # When
    response = await client.execute_workflow(
        ExtractMarkdownContentWorkflow,
        args,
        id=wf_id,
        task_queue=TaskQueues.WORKFLOWS,
    )

    # Then
    response = response.model_dump()
    expected_res = MarkdownExtractResponse(
        processed=ProcessingReportWithPages(n_docs=2, n_pages=3),
        successes=ProcessingReportWithPages(n_docs=2, n_pages=3),
        errors=ErrorReportWithPages(n_docs=0, n_pages=0),
    ).model_dump()
    assert response == expected_res
