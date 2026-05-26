import json
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from datashare_python.objects import DocArtifact
from datashare_python.types_ import TemporalClient
from datashare_python.utils import activity_defn, positional_args_only, write_artifact
from datashare_python.worker import datashare_worker
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError


@positional_args_only
def hello_world_keyword(*, who: str) -> str:
    return f"hello {who}"


def test_keyword_safe_activity() -> None:
    # When
    try:
        activity.defn(hello_world_keyword)
    except Exception as e:
        raise AssertionError(
            "couldn't create activity from keyword only function "
        ) from e


@activity_defn(name="non_retriable")
async def non_retriable() -> None:
    raise ValueError("non retriable error occurred")


@workflow.defn(name="non_retriable_workflow")
class NonRetriableWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            non_retriable,
            task_queue="io",
            schedule_to_close_timeout=timedelta(seconds=30),
        )


async def test_retriable(test_temporal_client_session: TemporalClient) -> None:
    # Given
    client = test_temporal_client_session
    workflow_id = f"workflow_{uuid.uuid4().hex}"
    worker_id = f"worker-{uuid.uuid4().hex}"
    worker = datashare_worker(
        client,
        worker_id=worker_id,
        task_queue="io",
        workflows=[NonRetriableWorkflow],
        activities=[non_retriable],
    )
    async with worker:
        with pytest.raises(WorkflowFailureError) as ctx:
            await client.execute_workflow(
                NonRetriableWorkflow.run, id=workflow_id, task_queue="io"
            )
        cause = ctx.value.cause.__cause__
        assert isinstance(cause, ApplicationError)
        assert cause.message == "non retriable error occurred"
        assert cause.non_retryable


def test_write_artifact(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    root_dir = Path(tmp_path)
    artifact_bytes = b"artifacts"
    filename = "some_artifact"
    metadata_key = "artifact_key"
    artifact = DocArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=artifact_bytes,
        filename=filename,
        metadata_key=metadata_key,
    )
    # When
    write_artifact(root_dir, artifact)
    # Then
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    meta_path = artifact_dir / "metadata.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta == {"artifact_key": filename}
    artifact_name = meta.get(metadata_key)
    assert artifact_name is not None
    artifact_path = artifact_dir / artifact_name
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == artifact_bytes


def test_write_artifact_with_existing_metadata(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    root_dir = Path(tmp_path)
    artifact_bytes = b"artifacts"
    filename = "some_artifact"
    metadata_key = "artifact_key"
    artifact = DocArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=artifact_bytes,
        filename=filename,
        metadata_key=metadata_key,
    )
    existing_metadata = {"some": "metadata"}
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    meta_path = artifact_dir / "metadata.json"
    meta_path.write_text(json.dumps(existing_metadata))
    # When
    write_artifact(root_dir, artifact)
    # Then
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    meta_path = artifact_dir / "metadata.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta == {"artifact_key": filename, "some": "metadata"}
    artifact_name = meta.get(metadata_key)
    assert artifact_name is not None
    artifact_path = artifact_dir / artifact_name
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == artifact_bytes
