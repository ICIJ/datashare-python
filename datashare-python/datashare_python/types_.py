from collections.abc import Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Protocol

from temporalio.client import Client

TemporalClient = Client


class ProgressRateHandler(Protocol):
    async def __call__(self, progress_rate: float) -> None:
        pass


class RawProgressHandler(Protocol):
    async def __call__(self, iteration: int) -> None:
        pass


FactoryReturnType = (
    AbstractContextManager
    | AbstractAsyncContextManager
    | Coroutine[None, None, None]
    | None
)


class ContextManagerFactory(Protocol):
    def __call__(*args, **_) -> FactoryReturnType: ...
