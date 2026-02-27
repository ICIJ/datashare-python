import asyncio
import inspect
import logging
import sys
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import partial, wraps
from inspect import signature
from typing import ParamSpec, TypeVar

import nest_asyncio
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
from temporalio.common import SearchAttributeKey
from temporalio.exceptions import ApplicationError

from .types_ import ProgressRateHandler, RawProgressHandler

DependencyLabel = str | None
DependencySetup = Callable[..., None]
DependencyAsyncSetup = Callable[..., Coroutine[None, None, None]]

PROGRESS_HANDLER_ARG = "progress"

P = ParamSpec("P")
T = TypeVar("T")


PROGRESS_SEARCH_ATTRIBUTE = SearchAttributeKey.for_text("Progress")
MAX_PROGRESS_SEARCH_ATTRIBUTE = SearchAttributeKey.for_keyword("MaxProgress")


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
            attributes = {
                PROGRESS_SEARCH_ATTRIBUTE.name: [progress],
                MAX_PROGRESS_SEARCH_ATTRIBUTE.name: [max_progress],
            }
            workflow.upsert_search_attributes(attributes)


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
                if supports_progress(activity_fn):
                    res = await activity_fn(self, *args, progress=handler)
                else:
                    res = await activity_fn(self, *args)
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
                event_loop.run_until_complete(handler(0.0))
                if supports_progress(activity_fn):
                    res = activity_fn(self, *args, progress=handler)
                else:
                    res = activity_fn(self, *args)
                event_loop.run_until_complete(handler(1.0))
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
        retriables = set()
    retriables = tuple(retriables)

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
    progress_weight: float = 1.0,
    retriables: set[type[Exception]] = None,
    name: str | None = None,
    *,
    no_thread_cancel_exception: bool = False,
    dynamic: bool = False,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(activity_fn: Callable[P, T]) -> Callable[P, T]:
        activity_fn = positional_args_only(activity_fn)
        activity_fn = with_retriables(retriables)(activity_fn)
        activity_fn = with_progress(progress_weight)(activity_fn)
        activity_fn = activity.defn(
            activity_fn,
            name=name,
            no_thread_cancel_exception=no_thread_cancel_exception,
            dynamic=dynamic,
        )
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
