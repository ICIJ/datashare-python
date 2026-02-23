import logging
from asyncio import AbstractEventLoop, iscoroutine
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar

from icij_common.es import ESClient

from .config import WorkerConfig
from .exceptions import DependencyInjectionError
from .task_client import DatashareTaskClient
from .types_ import ContextManagerFactory, TemporalClient

logger = logging.getLogger(__name__)


EVENT_LOOP: ContextVar[AbstractEventLoop] = ContextVar("event_loop")
ES_CLIENT: ContextVar[ESClient] = ContextVar("es_client")
TASK_CLIENT: ContextVar[DatashareTaskClient] = ContextVar("task_client")
TEMPORAL_CLIENT: ContextVar[TemporalClient] = ContextVar("temporal_client")


def set_event_loop(event_loop: AbstractEventLoop, **_) -> None:
    EVENT_LOOP.set(event_loop)


def lifespan_event_loop() -> AbstractEventLoop:
    try:
        return EVENT_LOOP.get()
    except LookupError as e:
        raise DependencyInjectionError("event loop") from e


def set_loggers(worker_config: WorkerConfig, worker_id: str, **_) -> None:
    worker_config.setup_loggers(worker_id=worker_id)
    logger.info("worker loggers ready to log 💬")
    logger.info("app config: %s", worker_config.model_dump_json(indent=2))


async def set_es_client(worker_config: WorkerConfig, **_) -> ESClient:
    client = worker_config.to_es_client()
    ES_CLIENT.set(client)
    return client


# Returns the globally injected ES client
def lifespan_es_client() -> ESClient:
    try:
        return ES_CLIENT.get()
    except LookupError as e:
        raise DependencyInjectionError("es client") from e


# Task client setup
async def set_task_client(worker_config: WorkerConfig, **_) -> DatashareTaskClient:
    task_client = worker_config.to_task_client()
    TASK_CLIENT.set(task_client)
    return task_client


# Returns the globally injected task client
def lifespan_task_client() -> DatashareTaskClient:
    try:
        return TASK_CLIENT.get()
    except LookupError as e:
        raise DependencyInjectionError("datashare task client") from e


# Temporal client setup
async def set_temporal_client(worker_config: WorkerConfig, **_) -> None:
    client = await worker_config.to_temporal_client()
    TEMPORAL_CLIENT.set(client)


# Returns the globally injected temporal client
def lifespan_temporal_client() -> TemporalClient:
    try:
        return TEMPORAL_CLIENT.get()
    except LookupError as e:
        raise DependencyInjectionError("temporal client") from e


@asynccontextmanager
async def with_dependencies(
    dependencies: list[ContextManagerFactory], **kwargs
) -> AsyncGenerator[None, None]:
    async with AsyncExitStack() as stack:
        for dep in dependencies:
            cm = dep(**kwargs)
            if hasattr(cm, "__aenter__"):
                await stack.enter_async_context(cm)
            elif hasattr(cm, "__enter__"):
                stack.enter_context(cm)
            elif iscoroutine(cm):
                await cm
        yield
