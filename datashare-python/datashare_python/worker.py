import inspect
import logging
import os
import socket
import sys
import threading
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from copy import copy
from typing import Any

from temporalio.worker import PollerBehaviorSimpleMaximum, Worker

from .config import WorkerConfig
from .dependencies import with_dependencies
from .discovery import Activity
from .types_ import ContextManagerFactory, TemporalClient

logger = logging.getLogger(__name__)

_TEMPORAL_CLIENT = "temporal_client"
_EVENT_LOOP = "event_loop"
_EXPECTED_INIT_ARGS = {"self", _TEMPORAL_CLIENT, _EVENT_LOOP, "args", "kwargs"}

_SEPARATE_IO_AND_CPU_WORKERS = """The worker will run sync (CPU-bound) activities as \
well as IO-bound workflows.
To avoid deadlocks due to the GIL, we advise to run all CPU-bound activities inside a \
worker and workflows + IO-bound activities in a separate worker. Please have a look \
at temporal's documentation for more details:
 https://docs.temporal.io/develop/python/python-sdk-sync-vs-async#the-python-asynchronous-event-loop-and-blocking-calls
"""

_SEPARATE_IO_AND_CPU_ACTIVITIES = """The worker will run sync (CPU-bound) activities \
as well as IO-bound activities (async def).

To avoid deadlocks due to the GIL, we advise to run all CPU-bound activities inside a \
worker and IO-bound activities in a separate worker. Please have a look at temporal's \
documentation for more details:
 https://docs.temporal.io/develop/python/python-sdk-sync-vs-async#the-python-asynchronous-event-loop-and-blocking-calls
"""

_ACTIVITY_THREAD_NAME_PREFIX = "datashare-activity-worker-"


class DatashareWorker(Worker):
    async def is_done(self) -> None:
        if self._async_context_run_task is None:
            raise ValueError("worker is not running")
        await self._async_context_run_task


def datashare_worker(
    client: TemporalClient,
    worker_id: str,
    *,
    workflows: list[type] | None = None,
    activities: list[Activity] | None = None,
    task_queue: str,
    # Scale horizontally be default for activities, each worker processes one activity
    # at a time
    max_concurrent_io_activities: int = 10,
) -> DatashareWorker:
    if workflows is None:
        workflows = []
    if activities is None:
        activities = []
    are_async = [a.__temporal_activity_definition.is_async for a in activities]
    if are_async and all(not a for a in are_async):
        activity_executor = ThreadPoolExecutor(
            thread_name_prefix=_ACTIVITY_THREAD_NAME_PREFIX
        )
    elif all(are_async):
        activity_executor = None  # Use temporal default
    else:
        activity_executor = ThreadPoolExecutor(
            thread_name_prefix=_ACTIVITY_THREAD_NAME_PREFIX
        )
        logger.warning(_SEPARATE_IO_AND_CPU_ACTIVITIES)

    max_concurrent_activities = max_concurrent_io_activities
    if isinstance(activity_executor, ThreadPoolExecutor):
        max_concurrent_activities = 1
        if workflows:
            logger.warning(_SEPARATE_IO_AND_CPU_WORKERS)

    return DatashareWorker(
        client,
        identity=worker_id,
        workflows=workflows,
        activities=activities,
        task_queue=task_queue,
        activity_executor=activity_executor,
        max_concurrent_activities=max_concurrent_activities,
        # Let's make sure we poll activities one at a time otherwise, this will reserve
        # activities and prevent other workers to poll and process them
        activity_task_poller_behavior=PollerBehaviorSimpleMaximum(1),
        # Workflow tasks are assumed to be very lightweight and fast we can reserve
        # several of them
        workflow_task_poller_behavior=PollerBehaviorSimpleMaximum(5),
    )


def create_worker_id(prefix: str) -> str:
    pid = os.getpid()
    threadid = threading.get_ident()
    hostname = socket.gethostname()
    # TODO: this might not be unique when using asyncio
    return f"{prefix}-{hostname}-{pid}-{threadid}"


def init_activity(
    activity: Callable, client: TemporalClient, event_loop: AbstractEventLoop
) -> Callable:
    is_object_method = "." not in activity.__qualname__
    if is_object_method:
        return activity
    cls = _get_class_from_method(activity)
    init_args = inspect.signature(cls.__init__).parameters
    invalid = [p for p in init_args if p not in _EXPECTED_INIT_ARGS]
    if invalid:
        msg = f"invalid activity arguments: {invalid}"
        raise ValueError(msg)
    kwargs = {_TEMPORAL_CLIENT: client, _EVENT_LOOP: event_loop}
    kwargs = {k: v for k, v in kwargs.items() if k in _EXPECTED_INIT_ARGS}
    if not kwargs:
        return activity
    act_instance = cls(**kwargs)
    act_method = getattr(act_instance, activity.__name__)
    return act_method


@asynccontextmanager
async def worker_context(
    worker_id: str,
    *,
    activities: list[Callable[..., Any] | None] | None = None,
    workflows: list[type] | None = None,
    worker_config: WorkerConfig,
    client: TemporalClient,
    event_loop: AbstractEventLoop,
    task_queue: str,
    dependencies: list[ContextManagerFactory] | None = None,
) -> AsyncGenerator[DatashareWorker, None]:
    discovered = []
    if activities is not None:
        discovered.extend(activities)
    if workflows is not None:
        discovered.extend(workflows)
    if dependencies is not None:
        discovered.extend(dependencies)
    discovered.append(worker_config)
    loggers = copy(worker_config.logging.loggers)
    discovered_loggers = {_get_object_package(o).__name__ for o in discovered}
    for logger in discovered_loggers:
        if logger not in loggers:
            # Log in info by default
            loggers[logger] = "INFO"
    deps_cm = (
        with_dependencies(
            dependencies,
            worker_config=worker_config,
            worker_id=worker_id,
            event_loop=event_loop,
            loggers=loggers,
        )
        if dependencies
        else _do_nothing_cm()
    )
    async with deps_cm:
        if activities is not None:
            acts = [
                init_activity(a, client=client, event_loop=event_loop)
                for a in activities
            ]
        else:
            acts = None
        worker = datashare_worker(
            client,
            worker_id,
            workflows=workflows,
            activities=acts,
            task_queue=task_queue,
            max_concurrent_io_activities=worker_config.max_concurrent_io_activities,
        )
        async with worker:
            yield worker


@asynccontextmanager
async def _do_nothing_cm() -> AsyncGenerator[None, None]:
    yield


def _get_class_from_method(method: Callable) -> type:
    class_name = method.__qualname__.rsplit(".", 1)[0]
    module = sys.modules[method.__module__]
    return getattr(module, class_name)


def _get_object_package(obj: Any) -> Any:
    mod = inspect.getmodule(obj)
    base, _, _ = mod.__name__.partition(".")
    return sys.modules[base]
