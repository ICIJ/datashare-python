import asyncio
import inspect
from typing import AsyncIterable, AsyncIterator, Awaitable, Callable, TypeVar

from aiostream import stream

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