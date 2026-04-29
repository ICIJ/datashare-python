import asyncio
import contextlib
import contextvars
import inspect
import json
import threading
from collections.abc import Awaitable, Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from functools import partial, wraps
from hashlib import sha256
from inspect import signature
from io import BytesIO
from pathlib import Path
from typing import Any, ParamSpec, TypeVar
from uuid import uuid4

import nest_asyncio
import temporalio
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ApplicationError

from .constants import METADATA_JSON
from .objects import DocArtifact
from .types_ import ProgressRateHandler, RawProgressHandler

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
    def __init__(self, temporal_client: Client, event_loop: asyncio.AbstractEventLoop):
        self._temporal_client = temporal_client
        nest_asyncio.apply()
        self._event_loop = event_loop


class WorkflowWithProgress:
    def __init__(self):
        self._progress: dict[tuple[str, str], Progress] = dict()
        self._update_lock = asyncio.Lock()

    @workflow.signal
    async def update_progress(self, signal: ProgressSignal) -> None:
        async with self._update_lock:
            # TODO: remove this log
            workflow.logger.debug("recording progress signal %s", signal)
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
        retry_policy = RetryPolicy(non_retryable_error_types=[])
    retry_policy = deepcopy(retry_policy)
    non_retryable_error_types = set(retry_policy.non_retryable_error_types)
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


async def progress_handler(
    progress: float,
    handle: WorkflowHandle,
    *,
    activity_id: str,
    run_id: str,
    weight: float = 1.0,
) -> None:
    signal = ProgressSignal(
        activity_id=activity_id, run_id=run_id, progress=progress, weight=weight
    )
    await handle.signal("update_progress", signal)
    with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
        activity.heartbeat()


def get_activity_progress_handler_async(
    client: Client, weight: float
) -> ProgressRateHandler:
    info = activity.info()
    run_id = info.workflow_run_id
    workflow_id = info.workflow_id
    activity_id = activity.info().activity_id
    workflow_handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    handler = partial(
        progress_handler,
        handle=workflow_handle,
        run_id=run_id,
        activity_id=activity_id,
        weight=weight,
    )
    return handler


def supports_progress(task_fn: Callable) -> bool:
    return any(
        param.name == PROGRESS_HANDLER_ARG
        for param in signature(task_fn).parameters.values()
    )


def with_progress(weight: float = 1.0) -> Callable[P, T]:
    if isinstance(weight, Callable):
        return with_progress(weight=1)(weight)

    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        # TODO: handle the fact activities should have only positional args...
        if asyncio.iscoroutinefunction(activity_fn):

            @wraps(activity_fn)
            async def wrapper(self: ActivityWithProgress, *args: P.args) -> T:
                if not isinstance(self, ActivityWithProgress):
                    msg = (
                        f"{with_progress.__name__} decorator is meant to be used on "
                        f"activities defined as an {ActivityWithProgress.__name__}"
                        f" method, expected a {ActivityWithProgress.__name__} as first"
                        f" argument, found {self}"
                    )
                    raise TypeError(msg)
                handler = get_activity_progress_handler_async(
                    client=self._temporal_client, weight=weight
                )
                await handler(0.0)
                res = await activity_fn(self, *args, progress=handler)
                await handler(1.0)
                return res

        else:

            @wraps(activity_fn)
            def wrapper(self: ActivityWithProgress, *args: P.args) -> T:
                if not isinstance(self, ActivityWithProgress):
                    msg = (
                        f"{with_progress.__name__} decorator is meant to be used on "
                        f"activities defined as an {ActivityWithProgress.__name__}"
                        f" method, expected a {ActivityWithProgress.__name__} as first"
                        f" argument, found {self}"
                    )
                    raise TypeError(msg)
                handler = get_activity_progress_handler_async(
                    client=self._temporal_client, weight=weight
                )
                event_loop = self._event_loop
                asyncio.run_coroutine_threadsafe(handler(0.0), event_loop).result()
                res = activity_fn(self, *args, progress=handler)
                asyncio.run_coroutine_threadsafe(handler(1.0), event_loop).result()
                return res

        return wrapper

    return decorator


def with_async_heartbeat(
    activity_fn: Callable[P, Awaitable[T]], n_missed_before_timeout: int
) -> Callable[P, Awaitable[T]]:
    # Copied from
    # https://github.com/temporalio/samples-python/blob/main/custom_decorator/activity_utils.py
    @wraps(activity_fn)
    async def wrapper(*args, **kwargs) -> T:
        heartbeat_timeout = activity.info().heartbeat_timeout
        heartbeat_task = None
        if heartbeat_timeout:
            period = heartbeat_timeout.total_seconds() / n_missed_before_timeout
            heartbeat_task = asyncio.create_task(_async_heartbeat_every(period))
        try:
            activity.heartbeat()
            return await activity_fn(*args, **kwargs)
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                await asyncio.wait([heartbeat_task])

    return wrapper


async def _async_heartbeat_every(period: float, *details: Any) -> None:
    with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
        activity.heartbeat(*details)
    while True:
        await asyncio.sleep(period)
        with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
            activity.heartbeat(*details)


def with_sync_heartbeat(
    activity_fn: Callable[P, T], n_missed_before_timeout: int
) -> Callable[P, T]:
    @wraps(activity_fn)
    def wrapper(*args, **kwargs) -> T:
        heartbeat_timeout = activity.info().heartbeat_timeout
        heartbeat_thread, stop_event = None, None
        if heartbeat_timeout:
            period = heartbeat_timeout.total_seconds() / n_missed_before_timeout
            ctx = contextvars.copy_context()
            run_args = (_sync_heartbeat_every, period, threading.Event())
            heartbeat_thread, stop_event = (
                threading.Thread(target=ctx.run, args=run_args),
                run_args[-1],
            )
            heartbeat_thread.start()
        try:
            return activity_fn(*args, **kwargs)
        finally:
            if heartbeat_thread:
                stop_event.set()
                heartbeat_thread.join()

    return wrapper


def _sync_heartbeat_every(
    period: float, stop_event: threading.Event, *details: Any
) -> None:
    with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
        activity.heartbeat(*details)
    while not stop_event.wait(period):
        with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
            activity.heartbeat(*details)


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
    name: str,
    progress_weight: float = 1.0,
    retriables: set[type[Exception]] = None,
    n_missed_heartbeats_before_timeout: int = 5,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        # TODO: some of these could probably be reimplemented more elegantly using
        #  temporal interceptors: https://docs.temporal.io/develop/python/workers/interceptors
        activity_fn = positional_args_only(activity_fn)
        activity_fn = with_retriables(retriables)(activity_fn)
        if supports_progress(activity_fn):
            activity_fn = with_progress(progress_weight)(activity_fn)
        is_async = asyncio.iscoroutinefunction(activity_fn)
        if is_async:
            activity_fn = with_async_heartbeat(
                activity_fn, n_missed_heartbeats_before_timeout
            )
        else:
            activity_fn = with_sync_heartbeat(
                activity_fn, n_missed_heartbeats_before_timeout
            )
        activity_fn = activity.defn(activity_fn, name=name)
        return activity_fn

    return decorator


def fatal_error_from_exception(exc: Exception) -> ApplicationError:
    exc_type = exc.__class__.__name__
    details = f"{exc_type}: {exc}"
    return ApplicationError(str(exc), details, type=exc_type, non_retryable=True)


def to_raw_progress(
    progress: ProgressRateHandler, max_progress: int
) -> RawProgressHandler:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    async def raw(p: int) -> None:
        await progress(p / max_progress)

    return raw


def to_scaled_progress(
    progress: ProgressRateHandler, *, start: float = 0.0, end: float = 1.0
) -> ProgressRateHandler:
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


def _read_artifact_metadata(root: Path, artifact: DocArtifact) -> dict:
    m_path = root / _metadata_path(artifact.filename, project=artifact.project)
    return json.loads(m_path.read_text())


def write_artifact(root: Path, artifact: DocArtifact) -> Path:
    artif_dir = root / artifacts_dir(artifact.doc_id, project=artifact.project)
    artif_dir.mkdir(exist_ok=True, parents=True)
    # TODO: if transcriptions are too large we could also serialize them
    #  as jsonl
    artifact_path: Path = artif_dir / artifact.filename
    if isinstance(artifact.artifact, bytes):
        artifact_path.write_bytes(artifact.artifact)
    elif isinstance(artifact_path, BytesIO):
        with artifact_path.open("wb") as f:
            f.write(artifact.artifact.read())
    meta_path = root / _metadata_path(artifact.doc_id, project=artifact.project)
    meta = _read_artifact_metadata(root, artifact) if meta_path.exists() else dict()
    meta[artifact.metadata_key] = artifact.filename
    meta_path.write_text(json.dumps(meta))
    return artifact_path.relative_to(artif_dir)


def debuggable_name(
    path: str, component_size_limit: int = 10, *, deterministic: bool = False
) -> str:
    path = Path(path)
    displayable_file_name = [c[:component_size_limit] for c in path.parts]
    uuid = sha256(str(path).encode()).hexdigest() if deterministic else uuid4().hex
    uuid = uuid[:20]
    return f"{uuid}-{'--'.join(displayable_file_name)}"


def activity_contextual_id(
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
