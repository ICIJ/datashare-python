import uuid

import pytest
from constants import CHUNK_ID
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    Document,
    ErrorReportWithPages,
    ProcessedFile,
    ProcessingReportWithPages,
)
from elasticsearch.dsl.query import MatchAll
from extract_core import DoclingPipelineConfig
from icij_common.es import ASC, QUERY, ESClient
from temporalio.client import Client as TemporalClient
from temporalio.client import WorkflowFailureError

from extract_worker.config import ExtractWorkerConfig
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

from .conftest import TEST_EMBEDDING_DIM


@pytest.mark.e2e
@pytest.mark.usefixtures("workflows_worker")
@pytest.mark.usefixtures("io_worker")
@pytest.mark.usefixtures("md_extract_cpu_worker")
async def test_extract_markdown_workflow_e2e(
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
@pytest.mark.usefixtures("workflows_worker")
@pytest.mark.usefixtures("io_worker")
@pytest.mark.usefixtures("embed_cpu_worker")
@pytest.mark.usefixtures("populate_es")
async def test_embed_workflow_e2e(
    test_temporal_client: TemporalClient,
    test_es_client: ESClient,
    chunk_index: str,
    doc_with_structure: ProcessedFile,
    doc_3: Document,
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-embed-{uuid.uuid4()}"
    args = EmbeddingArgs(project=TEST_PROJECT, docs=None, dim=TEST_EMBEDDING_DIM)

    # When
    response = await client.execute_workflow(
        EmbedDocumentsWorkflow, args, id=wf_id, task_queue=TaskQueues.WORKFLOWS
    )

    # Then
    assert response.successes == ProcessingReportWithPages(n_docs=2, n_pages=3)
    assert response.errors.n_docs == 0
    assert response.n_chunks == 4
    hits = []
    sort = [{CHUNK_ID: ASC}]
    body = {QUERY: MatchAll().to_dict()}
    search = test_es_client.poll_search_pages(index=chunk_index, body=body, sort=sort)
    async for res in search:
        hits += res["hits"]["hits"]
    doc_ids = sorted(h["_source"]["docId"] for h in hits)
    assert doc_ids == sorted([doc_with_structure.doc_id] * 2 + [doc_3.id] * 2)
    assert all(len(h["_source"]["embedding"]) == 64 for h in hits)


@pytest.mark.e2e
@pytest.mark.usefixtures("workflows_worker")
@pytest.mark.usefixtures("io_worker")
@pytest.mark.usefixtures("embed_cpu_worker")
@pytest.mark.usefixtures("populate_es")
@pytest.mark.usefixtures("doc_with_structure")
async def test_embed_workflow_should_fail_early_for_size_mismatch(
    test_temporal_client: TemporalClient, test_worker_config: ExtractWorkerConfig
) -> None:
    # Given
    client = test_temporal_client
    wf_id = f"extract-embed-{uuid.uuid4()}"
    args = EmbeddingArgs(project=TEST_PROJECT, docs=None, dim=372)

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
