import asyncio
import contextlib
import fcntl
import inspect
import json
import os
import shutil
import threading
import time
from collections.abc import Callable, Coroutine, Generator, Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, ParamSpec, TypeVar
from uuid import uuid4

import temporalio
from pydantic import ValidationError
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.contrib.pydantic import PydanticJSONPlainPayloadConverter, ToJsonOptions
from temporalio.converter import (
    CompositePayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    JSONPlainPayloadConverter,
)
from temporalio.exceptions import ApplicationError

from datashare_python.types_ import (
    AsyncProgressRateHandler,
    RawSyncProgressHandler,
    SyncProgressRateHandler,
)

from .constants import MANIFEST_JSON, METADATA_JSON
from .objects import DocArtifact, DocumentLocation, FilesystemDocument
from .types_ import RawAsyncProgressHandler

DependencyLabel = str | None
DependencySetup = Callable[..., None]
DependencyAsyncSetup = Callable[..., Coroutine[None, None, None]]

PROGRESS_HANDLER_ARG = "progress"

P = ParamSpec("P")
T = TypeVar("T")

_NEVER_RETRIABLES = {
    "ValidationError",
    "TypeError",
    "PydanticSchemaGenerationError",
    "PydanticSerializationError",
}


_ARTIFACT_LOCK = threading.Lock()
# For test
_LOCKED = threading.Event()


@dataclass(frozen=True)
class Progress:
    max_progress: float
    current: float = 0.0

    @property
    def progress(self) -> float:
        if self.max_progress == 0.0:
            return 0.0
        return self.current / self.max_progress


@dataclass(frozen=True)
class ProgressSignal:
    run_id: str
    activity_id: str
    progress: float = 0.0  # TODO: add validation that it should be between 0..1
    weight: float = 1.0

    def to_progress(self) -> Progress:
        return Progress(current=self.progress * self.weight, max_progress=self.weight)


class ActivityWithProgress:
    def __init__(
        self,
        temporal_client: Client,
        event_loop: asyncio.AbstractEventLoop | None = None,
    ):
        self._temporal_client = temporal_client
        self._event_loop = event_loop or asyncio.get_event_loop()


class WorkflowWithProgress:
    def __init__(self):
        self._progress: dict[tuple[str, str], Progress] = dict()
        self._update_lock = asyncio.Lock()

    @workflow.signal
    async def update_progress(self, signal: ProgressSignal) -> None:
        async with self._update_lock:
            # TODO: remove this log
            key = (signal.run_id, signal.activity_id)
            self._progress[key] = signal.to_progress()
            progress = sum(p.current for p in self._progress.values())
            max_progress = sum(p.max_progress for p in self._progress.values())
            attributes = [
                SearchAttributeKey.for_float("Progress").value_set(progress),
                SearchAttributeKey.for_float("MaxProgress").value_set(max_progress),
            ]
            workflow.upsert_search_attributes(attributes)


def _retry_policy_with_default(retry_policy: RetryPolicy | None) -> RetryPolicy:
    if retry_policy is None:
        retry_policy = RetryPolicy(non_retryable_error_types=[], maximum_attempts=3)
    retry_policy = deepcopy(retry_policy)
    non_retryable_error_types = (
        retry_policy.non_retryable_error_types
        if retry_policy.non_retryable_error_types is not None
        else []
    )
    non_retryable_error_types = set(non_retryable_error_types)
    non_retryable_error_types.update(_NEVER_RETRIABLES)
    retry_policy.non_retryable_error_types = list(non_retryable_error_types)
    return retry_policy


async def execute_activity(
    activity: Callable,
    task_queue: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: list | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta = timedelta(minutes=1),
    retry_policy: temporalio.common.RetryPolicy | None = None,
) -> Any:
    if args is None:
        args = []
    retry_policy = _retry_policy_with_default(retry_policy)
    return await workflow.execute_activity(
        activity,
        arg=arg,
        args=args,
        start_to_close_timeout=start_to_close_timeout,
        task_queue=task_queue,
        retry_policy=retry_policy,
        heartbeat_timeout=heartbeat_timeout,
    )


def positional_args_only(activity_fn: Callable[P, T]) -> Callable[P, T]:
    sig = inspect.signature(activity_fn)

    # Keep track of kwargs-only
    params = list(sig.parameters.values())
    keyword_only = {p.name for p in params if p.kind == inspect.Parameter.KEYWORD_ONLY}

    if asyncio.iscoroutinefunction(activity_fn):

        @wraps(activity_fn)
        async def wrapper(*args, **kwargs) -> T:
            # recreate kwargs from pargs
            new_args, new_kwargs = _unpack_positional_args(args, keyword_only, params)
            return await activity_fn(*new_args, **new_kwargs, **kwargs)

    else:

        @wraps(activity_fn)
        def wrapper(*args, **kwargs) -> T:
            # recreate kwargs from pargs
            new_args, new_kwargs = _unpack_positional_args(args, keyword_only, params)
            return activity_fn(*new_args, **new_kwargs, **kwargs)

    # Update the decorated function signature to appear as p-args only
    new_params = [
        p.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
        if p.kind == inspect.Parameter.KEYWORD_ONLY
        else p
        for p in params
    ]
    wrapper.__signature__ = sig.replace(parameters=new_params)

    return wrapper


def _unpack_positional_args(
    args: tuple, keyword_only_names: set[str], params: list
) -> tuple[list, dict]:
    new_args = []
    new_kwargs = dict()
    for value, param in zip(args, params, strict=False):
        if param.name in keyword_only_names:
            new_kwargs[param.name] = value
        else:
            new_args.append(value)
    return new_args, new_kwargs


def with_retriables(
    retriables: set[type[Exception]] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    if retriables is None:

        def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
            if asyncio.iscoroutinefunction(activity_fn):

                @wraps(activity_fn)
                async def wrapper(*args, **kwargs) -> T:
                    try:
                        return await activity_fn(*args, **kwargs)
                    except Exception as e:
                        raise fatal_error_from_exception(e) from e
            else:

                @wraps(activity_fn)
                def wrapper(*args, **kwargs) -> T:
                    try:
                        return activity_fn(*args, **kwargs)
                    except Exception as e:
                        raise fatal_error_from_exception(e) from e

            return wrapper

        return decorator

    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        if asyncio.iscoroutinefunction(activity_fn):

            @wraps(activity_fn)
            async def wrapper(*args, **kwargs) -> T:
                try:
                    return await activity_fn(*args, **kwargs)
                except retriables:
                    raise
                except Exception as e:
                    raise fatal_error_from_exception(e) from e

        else:

            @wraps(activity_fn)
            def wrapper(*args, **kwargs) -> T:
                try:
                    return activity_fn(*args, **kwargs)
                except retriables:
                    raise
                except Exception as e:
                    raise fatal_error_from_exception(e) from e

        return wrapper

    return decorator


def activity_defn(
    name: str, retriables: set[type[Exception]] = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:

    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        activity_fn = positional_args_only(activity_fn)
        activity_fn = with_retriables(retriables)(activity_fn)
        activity_fn = activity.defn(activity_fn, name=name)

        is_async = asyncio.iscoroutinefunction(activity_fn)
        if is_async:

            @wraps(activity_fn)
            async def wrapper(*args, **kwargs) -> T:
                return await activity_fn(*args, **kwargs)
        else:

            @wraps(activity_fn)
            def wrapper(*args, **kwargs) -> T:
                return activity_fn(*args, **kwargs)

        return wrapper

    return decorator


def fatal_error_from_exception(exc: Exception) -> ApplicationError:
    exc_type = exc.__class__.__name__
    details = f"{exc_type}: {exc}"
    return ApplicationError(str(exc), details, type=exc_type, non_retryable=True)


def to_raw_async_progress(
    progress: AsyncProgressRateHandler, max_progress: int
) -> RawAsyncProgressHandler:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    async def raw(p: int) -> None:
        await progress(p / max_progress)

    return raw


def to_raw_sync_progress(
    progress: SyncProgressRateHandler, max_progress: int
) -> RawSyncProgressHandler:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    def raw(iteration: int, event_loop: asyncio.AbstractEventLoop) -> None:
        progress(iteration / max_progress, event_loop)

    return raw


def to_scaled_async_progress(
    progress: AsyncProgressRateHandler, *, start: float = 0.0, end: float = 1.0
) -> AsyncProgressRateHandler:
    if not 0 <= start < end:
        raise ValueError("start must be [0, end[")
    if not start < end <= 1.0:
        raise ValueError("end must be ]start, 1.0]")

    async def _scaled(p: float) -> None:
        await progress(start + p * (end - start))

    return _scaled


def safe_dir(doc_id: str) -> Path:
    if len(doc_id) < 4:
        raise ValueError(f"expected doc_id to be at least 4, found {doc_id}")
    parts = (p for p in (doc_id[:2], doc_id[2:4]) if p)
    return Path(*parts)


def artifacts_dir(doc_id: str, *, project: str) -> Path:
    return Path(project, safe_dir(doc_id), doc_id)


def _metadata_path(doc_id: str, *, project: str) -> Path:
    metadata_path = artifacts_dir(doc_id, project=project) / METADATA_JSON
    return metadata_path


def _manifest_path(doc_id: str, *, project: str) -> Path:
    manifest_path = artifacts_dir(doc_id, project=project) / MANIFEST_JSON
    return manifest_path


def _read_artifact_manifest(root: Path, artifact: DocArtifact) -> dict:
    m_path = root / _manifest_path(artifact.doc_id, project=artifact.project)
    if not m_path.exists():
        m_path = root / _metadata_path(artifact.doc_id, project=artifact.project)
        if not m_path.exists():
            msg = f"couldn't find manifest nor metadata for {artifact.doc_id}"
            raise FileNotFoundError(msg)
    return json.loads(m_path.read_text())


def write_artifact(
    root: Path, artifact: DocArtifact, lock_timeout_ms: int = 30_000
) -> Path:
    # TODO: WARNING many writers could write at the time, to avoid inconsistent
    #  states we should handle this somehow
    artif_dir = root / artifacts_dir(artifact.doc_id, project=artifact.project)
    artif_dir.mkdir(exist_ok=True, parents=True)
    artifact_path = artif_dir / artifact.filename
    # We're using POSIX locks which are not exclusive to filedescriptors
    # but to processes, we hence have to ensure several threads from the same
    # Python process acquire the lock at the same time. We're using a python mutex lock
    with artifact_lock(artif_dir, lock_timeout_ms):
        # Read the metadata first (things could go wrong here in case someone is reading
        # at the same time). We read in a backward compatible wat and write to that same
        # location. We don't take responsibility for migrating the data, the DS back
        # will do it
        manifest_path, manifest = _read_manifest_backward_compatible(root, artifact)
        is_legacy = manifest_path.name == "metadata.json"
        # Pop the status key from the manifest before writing
        manifest_entry = manifest.get(artifact.type)
        if manifest_entry is not None and not is_legacy:
            manifest[artifact.type].pop("status", None)
            manifest_path.write_text(json.dumps(manifest))
        # Write the artifact
        _write_artifact_bytes(artifact_path, artifact.artifact)
        # Update the manifest entry with details and new states
        if is_legacy:
            manifest_entry = str(artifact_path.relative_to(artif_dir))
        else:
            manifest_entry = artifact.manifest_entry.model_dump(
                mode="json", by_alias=True
            )
        manifest[artifact.type] = manifest_entry
        manifest_path.write_text(json.dumps(manifest))
        return artifact_path.relative_to(artif_dir)


@contextlib.contextmanager
def _set_event() -> Generator[None, None, None]:
    try:
        _LOCKED.set()
        yield
    finally:
        _LOCKED.clear()


@contextlib.contextmanager
def artifact_lock(
    artifacts_dir: Path, lock_timeout_ms: int
) -> Generator[None, None, None]:
    lock_path = artifacts_dir / f"{MANIFEST_JSON}.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY)
    with _ARTIFACT_LOCK, _set_event():
        start = time.time()
        timeout_s = lock_timeout_ms / 1000
        while time.time() - start < timeout_s:
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                # TODO: we shouldn't block here, use asyncio instead
                time.sleep(0.02)
                continue
            yield
            return
        msg = (
            f"failed to acquire lock on {lock_path} in less than {lock_timeout_ms} ms."
        )
        raise TimeoutError(msg)


def _read_manifest_backward_compatible(
    root: Path, artifact: DocArtifact
) -> tuple[Path, dict[str, Any]]:
    manifest_path = root / _manifest_path(artifact.doc_id, project=artifact.project)
    if manifest_path.exists():
        return manifest_path, _read_artifact_manifest(root, artifact)
    meta_path = root / _metadata_path(artifact.doc_id, project=artifact.project)
    if meta_path.exists():
        return meta_path, _read_artifact_manifest(root, artifact)
    return manifest_path, dict()


def _write_artifact_bytes(path: Path, artifact: bytes | BytesIO | Path) -> None:
    match artifact:
        case bytes():
            path.write_bytes(artifact)
        case BytesIO():
            with path.open("wb") as f:
                f.write(artifact.read())
        case Path():
            path.unlink(missing_ok=True)
            shutil.move(artifact, path)
        case _:
            msg = f"unsupported artifact type: {artifact.__class__.__name__}"
            raise ValueError(msg)


def debuggable_name(
    path: str, component_size_limit: int = 10, *, deterministic: bool = False
) -> str:
    path = Path(path)
    displayable_file_name = [c[:component_size_limit] for c in path.parts]
    uuid = sha256(str(path).encode()).hexdigest() if deterministic else uuid4().hex
    uuid = uuid[:20]
    return f"{uuid}-{'--'.join(displayable_file_name)}"


def contextual_id(
    *, wf_context: bool = True, act_context: bool = False, run_context: bool = False
) -> str:
    contextual_id = []
    act_info = activity.info()
    if not wf_context and not act_context:
        raise ValueError("at least one of wf_context and act_context must be True")
    if wf_context:
        contextual_id.append(act_info.workflow_id)
        if run_context:
            contextual_id.append(act_info.workflow_run_id)
    if act_context:
        contextual_id.append(act_info.activity_type)
        contextual_id.append(act_info.activity_id)
        if run_context:
            contextual_id.append(act_info.activity_run_id)
    return "-".join(contextual_id)


# TODO: deprecated, remove me at the next breaking
activity_contextual_id = contextual_id


def _contextual_path(
    *, wf_context: bool = True, act_context: bool = True, run_context: bool = False
) -> Path:
    act_info = activity.info()
    path = []
    if not wf_context and not act_context:
        raise ValueError("at least one of wf_context and act_context must be True")
    if wf_context:
        path = [act_info.workflow_type]
        path.append(act_info.workflow_id)
        if run_context:
            path.append(act_info.workflow_run_id)
    if act_context:
        path.append(act_info.activity_type)
        path.append(act_info.activity_id)
        if run_context:
            path.append(act_info.activity_run_id)
    return Path(*path)


def activity_workdir(
    workdir: Path,
    project: str,
    *,
    wf_context: bool = True,
    act_context: bool = True,
    run_context: bool = False,
) -> Path:
    ctx_path = _contextual_path(
        wf_context=wf_context, act_context=act_context, run_context=run_context
    )
    return workdir.joinpath(project, ctx_path)


def symlink_embedded_document_to_workdir(
    doc: FilesystemDocument, artifacts_root: Path, *, workdir: Path
) -> FilesystemDocument:
    match doc.location:
        case DocumentLocation.ARTIFACTS:
            symlinks_dir = workdir / doc.index / "symlinks"
            symlinks_dir.mkdir(parents=True, exist_ok=True)
            symlink_path = Path(*doc.path.parts[:-1], doc.id)
            # Replace the "raw" with the doc id
            doc_ext = Path(doc.resource_name).suffix
            symlink_path = symlink_path.relative_to(Path(doc.index))
            symlink_path = symlinks_dir / f"{symlink_path}{doc_ext}"
            symlink_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path = artifacts_root / doc.path
            with contextlib.suppress(FileExistsError):
                os.symlink(artifact_path, symlink_path)
            return FilesystemDocument(
                path=symlink_path.relative_to(workdir),
                id=doc.id,
                location=DocumentLocation.WORKDIR,
                index=doc.index,
                resource_name=doc.resource_name,
            )
        case DocumentLocation.ORIGINAL:
            return doc
        case _:
            raise ValueError(f"unsupported location {doc.location}")


def read_jsonl(path: Path) -> Iterable[dict]:
    with path.open() as f:
        for line in f:
            line = line.strip()  # noqa: PLW2901
            if line:
                yield json.loads(line)


class _PydanticPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        json_payload_converter = PydanticJSONPlainPayloadConverter(
            ToJsonOptions(exclude_unset=False)
        )
        super().__init__(
            *(
                c
                if not isinstance(c, JSONPlainPayloadConverter)
                else json_payload_converter
                for c in DefaultPayloadConverter.default_encoding_payload_converters
            )
        )

    def from_payloads(
        self, payloads: Sequence[Payload], type_hints: list[type] | None = None
    ) -> list[Any]:
        try:
            return super().from_payloads(payloads, type_hints)
        except (TypeError, ValidationError) as e:
            raise fatal_error_from_exception(e) from e

    def to_payloads(self, values: Sequence[Any]) -> list[Payload]:
        try:
            return super().to_payloads(values)
        except (TypeError, ValidationError) as e:
            raise fatal_error_from_exception(e) from e


PYDANTIC_DATA_CONVERTER = DataConverter(
    payload_converter_class=_PydanticPayloadConverter
)
