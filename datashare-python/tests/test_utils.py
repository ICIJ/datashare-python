import asyncio
import fcntl
import json
import os
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest
from datashare_python.constants import MANIFEST_JSON
from datashare_python.objects import (
    ArtifactType,
    DatashareModel,
    DocArtifact,
    ManifestEntry,
    TaskArgs,
)
from datashare_python.types_ import TemporalClient
from datashare_python.utils import (
    _LOCKED,
    SharedResources,
    activity_defn,
    artifact_lock,
    positional_args_only,
    write_artifact,
)
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


class DeserArg(DatashareModel):
    value: str


@activity_defn(name="non_retriable")
async def deser_test_act(arg: DeserArg) -> str:
    return arg.value


@workflow.defn(name="non_retriable_workflow")
class NonRetriableWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            non_retriable,
            task_queue="io",
            schedule_to_close_timeout=timedelta(seconds=30),
        )


@workflow.defn(name="test-deserialization-error")
class TestDeserializationErrorWorkflow:
    @workflow.run
    async def run(self, args: DeserArg) -> None:
        await workflow.execute_activity(
            deser_test_act,
            args=[args],
            task_queue="deser",
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


async def test_deserialization_error(
    test_temporal_client_session: TemporalClient,
) -> None:
    # Given
    wrong_args = {"valueee": "some-value"}
    client = test_temporal_client_session
    workflow_id = f"workflow_{uuid.uuid4().hex}"
    worker_id = f"worker-{uuid.uuid4().hex}"
    worker = datashare_worker(
        client,
        worker_id=worker_id,
        task_queue="deser",
        workflows=[TestDeserializationErrorWorkflow],
        activities=[deser_test_act],
    )
    async with worker:
        with pytest.raises(WorkflowFailureError) as ctx:
            await client.execute_workflow(
                TestDeserializationErrorWorkflow.run,
                args=[wrong_args],
                id=workflow_id,
                task_queue="deser",
            )
        assert ctx.value.cause.non_retryable
        root_cause = ctx.value.cause.__cause__
        assert isinstance(root_cause, ApplicationError)
        assert "2 validation errors for DeserArg" in root_cause.message


class MockedArgs(TaskArgs):
    some_value: str


class MockedManifestEntry(ManifestEntry): ...


class MockedArtifact(DocArtifact):
    filename: ClassVar[str] = "mocked-structure"
    type: ClassVar[ArtifactType] = ArtifactType.STRUCTURE


def test_write_artifact(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    args = MockedArgs(some_value="value")
    root_dir = Path(tmp_path)
    artifact_bytes = b"artifacts"
    manifest_entry = MockedManifestEntry.complete(args)
    artifact = MockedArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=artifact_bytes,
        manifest_entry=manifest_entry,
    )
    # When
    write_artifact(root_dir, artifact)
    # Then
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    manifest_path = artifact_dir / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    expected_manifest = {
        "structure": {
            "status": "complete",
            "taskInput": {"someValue": "value"},
            "label": None,
        }
    }
    assert manifest == expected_manifest
    artifact_path = artifact_dir / "mocked-structure"
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == artifact_bytes


def test_write_artifact_with_existing_metadata(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    args = MockedArgs(some_value="value")
    root_dir = Path(tmp_path)
    artifact_bytes = b"artifacts"
    manifest_entry = MockedManifestEntry.complete(args)
    artifact = MockedArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=artifact_bytes,
        manifest_entry=manifest_entry,
    )
    existing_manifest = {"some": "value"}
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps(existing_manifest))
    # When
    write_artifact(root_dir, artifact)
    # Then
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    expected_manifest = {
        "structure": {
            "status": "complete",
            "taskInput": {"someValue": "value"},
            "label": None,
        },
        "some": "value",
    }
    assert manifest == expected_manifest
    assert manifest == expected_manifest
    artifact_path = artifact_dir / "mocked-structure"
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == artifact_bytes


def test_write_artifact_with_existing_legacy_metadata(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    args = MockedArgs(some_value="value")
    root_dir = Path(tmp_path)
    artifact_bytes = b"artifacts"
    manifest_entry = MockedManifestEntry.complete(args)
    artifact = MockedArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=artifact_bytes,
        manifest_entry=manifest_entry,
    )
    existing_metadata = {"structure": "existing-structure"}
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
    assert meta == {"structure": MockedArtifact.filename}
    artifact_name = meta.get(ArtifactType.STRUCTURE)
    assert artifact_name is not None
    artifact_path = artifact_dir / artifact_name
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == artifact_bytes


def test_overwrite_artifact(tmp_path: Path) -> None:
    from datashare_python.conftest import TEST_PROJECT  # noqa: PLC0415

    # Given
    args = MockedArgs(some_value="value")
    root_dir = Path(tmp_path)
    manifest_entry = MockedManifestEntry.complete(args)
    first = MockedArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=b"first",
        manifest_entry=manifest_entry,
    )
    second = MockedArtifact(
        project=TEST_PROJECT,
        doc_id="doc_id",
        artifact=b"second",
        manifest_entry=manifest_entry,
    )
    write_artifact(root_dir, first)
    # When
    write_artifact(root_dir, second)
    # Then
    artifact_dir = root_dir / TEST_PROJECT / "do" / "c_" / "doc_id"
    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    manifest_path = artifact_dir / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    expected_manifest = {
        "structure": {
            "status": "complete",
            "taskInput": {"someValue": "value"},
            "label": None,
        },
    }
    assert manifest == expected_manifest
    assert manifest == expected_manifest
    artifact_path = artifact_dir / "mocked-structure"
    assert artifact_path.exists()
    assert artifact_path.read_bytes() == b"second"


def _acquire_lock(artifact_dir: Path, timeout_ms: int) -> str:
    with artifact_lock(artifact_dir, timeout_ms):
        return "acquired"


def test_artifact_lock_file(tmp_path: Path) -> None:
    # Given
    timeout_ms = 100
    artifact_dir = Path(tmp_path)
    lock_path = artifact_dir / f"{MANIFEST_JSON}.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY)
    # The file lock is a POSIX file lock and doesn't work intra process
    pool = ProcessPoolExecutor(max_workers=1)
    # When
    with pool:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f = pool.submit(_acquire_lock, artifact_dir, timeout_ms)
    # Then
    expected = "failed to acquire lock"
    with pytest.raises(TimeoutError, match=expected):
        f.result()


async def _sleep_with_lock(
    sleep_s: int, artifact_dir: Path, *, timeout_ms: int
) -> None:
    with artifact_lock(artifact_dir, timeout_ms):
        await asyncio.sleep(sleep_s)


async def test_artifact_lock_in_process(tmp_path: Path) -> None:
    # Given
    sleep_s = 1
    timeout_ms = 100
    artifact_dir = Path(tmp_path)
    # When
    asyncio.create_task(_sleep_with_lock(sleep_s, artifact_dir, timeout_ms=timeout_ms))
    # Then
    completed_without_timeout = _LOCKED.wait(sleep_s / 2)
    assert not completed_without_timeout


def test_get_or_cache_resource_new_key() -> None:
    # Given
    shared = SharedResources()
    new_key = "k"

    def factory() -> str:
        return "cached"

    # When
    from_cache = shared.get_or_cache_resource(new_key, factory)
    # Then
    assert from_cache == "cached"


def test_get_or_cache_resource_existing_key() -> None:
    # Given
    shared = SharedResources()
    existing_key = "k"

    def factory() -> str:
        return uuid.uuid4().hex

    # When
    first = shared.get_or_cache_resource(existing_key, factory)
    second = shared.get_or_cache_resource(existing_key, factory)
    # Then
    assert second == first


def test_get_or_cache_eviction_callback_is_called_on_exit() -> None:
    # Given
    mock = MagicMock()
    eviction_callback = mock.evict
    key = "k"
    factory = lambda: "value"  # noqa: E731
    shared = SharedResources(eviction_callback=eviction_callback)
    # When
    with shared:
        shared.get_or_cache_resource(key, factory)
    # Then
    eviction_callback.assert_called_once_with(key, "value")
