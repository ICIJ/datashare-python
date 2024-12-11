import asyncio
import inspect
from itertools import islice
from typing import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, TypeVar

from aiostream import stream
from icij_worker.ds_task_client import DatashareTaskClient

T = TypeVar("T")

Predicate = Callable[[T], bool] | Callable[[T], Awaitable[bool]]


async def async_batches(
    iterable: AsyncIterable[T], batch_size: int
) -> AsyncIterator[tuple[T]]:
    if batch_size < 1:
        raise ValueError("n must be at least one")
    while True:
        batch = []
        async for item in stream.take(iterable, batch_size):
            batch.append(item)
        if not batch:
            return
        yield tuple(batch)


def batches(iterable: Iterable[T], batch_size: int):
    if batch_size < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, batch_size)):
        yield batch


async def maybe_await(maybe_awaitable: Awaitable[T] | T) -> T:
    if inspect.isawaitable(maybe_awaitable):
        return await maybe_awaitable
    return maybe_awaitable


async def once(item: T) -> AsyncIterator[T]:
    yield item


def before_and_after(
    iterable: AsyncIterable[T], predicate: Predicate[T]
) -> tuple[AsyncIterable[T], AsyncIterable[T]]:
    transition = asyncio.get_event_loop().create_future()

    async def true_iterator():
        async for elem in iterable:
            if await maybe_await(predicate(elem)):
                yield elem
            else:
                transition.set_result(elem)
                return
        transition.set_exception(StopAsyncIteration)

    async def remainder_iterator():
        try:
            yield await transition
        except StopAsyncIteration:
            return
        async for elm in iterable:
            yield elm

    return true_iterator(), remainder_iterator()


class DSTaskClient(DatashareTaskClient):

    async def __aenter__(self):
        await super().__aenter__()

        async with self._get("/settings") as res:
            # SimpleCookie doesn't seem to parse DS cookie so we perform some dirty
            # hack here
            session_id = [
                item
                for item in res.headers["Set-Cookie"].split("; ")
                if "session_id" in item
            ]
            if len(session_id) != 1:
                raise ValueError("Invalid cookie")
            k, v = session_id[0].split("=")
            self._session.cookie_jar.update_cookies({k: v})
