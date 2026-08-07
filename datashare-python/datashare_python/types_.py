import asyncio
from collections.abc import Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from temporalio.client import Client

TemporalClient = Client


class AsyncProgressRateHandler(Protocol):
    async def __call__(self, progress_rate: float, *, force: bool = False) -> None: ...


class SyncProgressRateHandler(Protocol):
    def __call__(
        self,
        progress_rate: float,
        event_loop: asyncio.AbstractEventLoop,
        *,
        force: bool = False,
    ) -> None: ...


ProgressRateHandler = SyncProgressRateHandler | AsyncProgressRateHandler


@dataclass
class Weight:
    value: float


class RawAsyncProgressHandler(Protocol):
    async def __call__(self, iteration: int, *, force: bool = False) -> None: ...


class RawSyncProgressHandler(Protocol):
    async def __call__(
        self,
        iteration: int,
        event_loop: asyncio.AbstractEventLoop,
        *,
        force: bool = False,
    ) -> None: ...


FactoryReturnType = (
    AbstractContextManager
    | AbstractAsyncContextManager
    | Coroutine[None, None, None]
    | None
)


class ContextManagerFactory(Protocol):
    def __call__(*args, **_) -> FactoryReturnType: ...
