import asyncio
import inspect
import json
import logging
import sys
from collections.abc import (
    Callable,
    Coroutine,
)
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
from icij_common.logging_utils import (
    DATE_FMT,
    STREAM_HANDLER_FMT,
    STREAM_HANDLER_FMT_WITH_WORKER_ID,
    WorkerIdFilter,
)
from icij_common.pydantic_utils import get_field_default_value
from pydantic.fields import FieldInfo
from pythonjsonlogger.json import JsonFormatter
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
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        activity_fn = positional_args_only(activity_fn)
        activity_fn = with_retriables(retriables)(activity_fn)
        if supports_progress(activity_fn):
            activity_fn = with_progress(progress_weight)(activity_fn)
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


class LogWithWorkerIDMixin:
    def setup_loggers(self, worker_id: str | None = None) -> None:
        # Ugly work around the Pydantic V1 limitations...
        all_loggers = self.loggers
        if isinstance(all_loggers, FieldInfo):
            all_loggers = get_field_default_value(all_loggers)
        all_loggers.append(__name__)
        loggers = sorted(set(all_loggers))
        log_level = self.log_level
        if isinstance(log_level, FieldInfo):
            log_level = get_field_default_value(log_level)
        force_warning = getattr(self, "force_warning_loggers", [])
        if isinstance(force_warning, FieldInfo):
            force_warning = get_field_default_value(force_warning)
        force_warning = set(force_warning)
        worker_id_filter = None
        if worker_id is not None:
            worker_id_filter = WorkerIdFilter(worker_id)
        handlers = self._handlers(worker_id_filter, log_level)
        for logger_ in loggers:
            logger_ = logging.getLogger(logger_)  # noqa: PLW2901
            level = getattr(logging, log_level)
            if logger_.name in force_warning:
                level = max(logging.WARNING, level)
            logger_.setLevel(level)
            logger_.handlers = []
            for handler in handlers:
                logger_.addHandler(handler)

    def _handlers(
        self, worker_id_filter: logging.Filter | None, log_level: int
    ) -> list[logging.Handler]:
        stream_handler = logging.StreamHandler(sys.stderr)
        if worker_id_filter is not None:
            fmt = STREAM_HANDLER_FMT_WITH_WORKER_ID
        else:
            fmt = STREAM_HANDLER_FMT
        log_in_json = getattr(self, "log_in_json", False)
        if isinstance(log_in_json, FieldInfo):
            log_in_json = get_field_default_value(log_in_json)
        if log_in_json:
            fmt = JsonFormatter(fmt, DATE_FMT)
        else:
            fmt = logging.Formatter(fmt, DATE_FMT)
        stream_handler.setFormatter(fmt)
        handlers = [stream_handler]
        for handler in handlers:
            if worker_id_filter is not None:
                handler.addFilter(worker_id_filter)
            handler.setLevel(log_level)
        return handlers


def safe_dir(doc_id: str) -> Path:
    if len(doc_id) < 4:
        raise ValueError(f"expected doc_id to be at least 4, found {doc_id}")
    parts = (p for p in (doc_id[:2], doc_id[2:4]) if p)
    return Path(*parts)


def _artifacts_dir(doc_id: str, *, project: str) -> Path:
    return Path(project, safe_dir(doc_id), doc_id)


def _metadata_path(doc_id: str, *, project: str) -> Path:
    metadata_path = _artifacts_dir(doc_id, project=project) / METADATA_JSON
    return metadata_path


def _read_artifact_metadata(root: Path, artifact: DocArtifact) -> dict:
    m_path = root / _metadata_path(artifact.filename, project=artifact.project)
    return json.loads(m_path.read_text())


def write_artifact(root: Path, artifact: DocArtifact) -> Path:
    artif_dir = root / _artifacts_dir(artifact.doc_id, project=artifact.project)
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
    return f"{uuid}-{'__'.join(displayable_file_name)}"


def activity_contextual_id(*, wf_context: bool = False) -> str:
    act_info = activity.info()
    act_id = act_info.activity_id
    act_run_id = act_info.activity_id
    act_type = act_info.activity_type
    contextual_id = f"{act_type}-{act_id}-{act_run_id}"
    if wf_context:
        wf_id = act_info.workflow_id
        wf_run_id = act_info.workflow_run_id
        contextual_id += f"-{wf_id}-{wf_run_id}"
    return contextual_id
