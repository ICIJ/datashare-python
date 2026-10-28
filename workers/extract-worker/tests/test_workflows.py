import uuid

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    Document,
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
)
from extract_core import DoclingPipelineConfig
from extract_worker.config import ExtractWorkerConfig
from extract_worker.embeddings import embeddings_index_body
from extract_worker.objects import (
    EmbeddingArgs,
    MarkdownExtractArgs,
    MarkdownExtractResponse,
)
from extract_worker.workflows import (
    EmbedDocumentsWorkflow,
    ExtractMarkdownContentWorkflow,
    TaskQueues,
)
from icij_common.es import ESClient
from temporalio.client import Client as TemporalClient
from temporalio.client import WorkflowFailureError
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


@pytest.mark.e2e
async def test_embed_workflow_e2e(  # noqa: PLR0917
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    embed_cpu_worker: Worker,  # noqa: ARG001
    populate_es: list[Document],  # noqa: ARG001
    test_temporal_client: TemporalClient,
    test_es_client: ESClient,
    test_worker_config: ExtractWorkerConfig,
    doc_with_structure: ProcessedFile,
    doc_3: Document,
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-embed-{uuid.uuid4()}"
    args = EmbeddingArgs(project=TEST_PROJECT, docs=None, dim=64)
    index = test_worker_config.embedding.index_name(TEST_PROJECT)
    index_body = embeddings_index_body(args.dim)
    await test_es_client.indices.create(index=index, body=index_body)

    # When
    response = await client.execute_workflow(
        EmbedDocumentsWorkflow,
        args,
        id=wf_id,
        task_queue=TaskQueues.WORKFLOWS,
    )

    # Then
    assert response.successes == ProcessingReportWithPages(n_docs=2, n_pages=3)
    assert response.errors.n_docs == 0
    assert response.n_chunks == 3
    res = await test_es_client.search(index=index, body={"query": {"match_all": {}}})
    hits = res["hits"]["hits"]
    doc_ids = sorted(h["_source"]["docId"] for h in hits)
    assert doc_ids == sorted([doc_with_structure.doc_id] * 2 + [doc_3.id])
    assert all(len(h["_source"]["embedding"]) == 64 for h in hits)


@pytest.mark.e2e
async def test_embed_workflow_should_fail_early_for_size_mismatch(  # noqa: PLR0917
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    embed_cpu_worker: Worker,  # noqa: ARG001
    populate_es: list[Document],  # noqa: ARG001
    test_temporal_client: TemporalClient,
    test_es_client: ESClient,
    test_worker_config: ExtractWorkerConfig,
    doc_with_structure: ProcessedFile,  # noqa: ARG001
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-embed-{uuid.uuid4()}"
    args = EmbeddingArgs(project=TEST_PROJECT, docs=None, dim=64)
    index = test_worker_config.embedding.index_name(TEST_PROJECT)
    index_body = embeddings_index_body(32)
    await test_es_client.indices.create(index=index, body=index_body)

    # When
    with pytest.raises(WorkflowFailureError) as exc:
        await client.execute_workflow(
            EmbedDocumentsWorkflow,
            args,
            id=wf_id,
            task_queue=TaskQueues.WORKFLOWS,
        )

    # Then
    assert "EmbeddingSizeMismatch" in str(exc.value.cause.cause)
    workdir = test_worker_config.paths.workdir
    assert not list(workdir.rglob("*-chunks.jsonl"))
