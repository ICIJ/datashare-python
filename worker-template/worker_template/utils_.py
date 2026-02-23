import asyncio
import inspect
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
)
from itertools import islice
from typing import TypeVar

T = TypeVar("T")

Predicate = Callable[[T], bool] | Callable[[T], Awaitable[bool]]


async def async_batches(
    iterable: AsyncIterable[T], batch_size: int
) -> AsyncIterator[tuple[T]]:
    it = aiter(iterable)
    if batch_size < 1:
        raise ValueError("n must be at least one")
    while True:
        batch = []
        while len(batch) < batch_size:
            try:
                batch.append(await anext(it))
            except StopAsyncIteration:
                if batch:
                    yield tuple(batch)
                return
        yield tuple(batch)


def batches(
    iterable: Iterable[T], batch_size: int
) -> Generator[tuple[T, ...], None, None]:
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

    async def true_iterator() -> AsyncIterator[T]:
        async for elem in iterable:
            if await maybe_await(predicate(elem)):
                yield elem
            else:
                transition.set_result(elem)
                return
        transition.set_exception(StopAsyncIteration)

    async def remainder_iterator() -> AsyncIterator[T]:
        try:
            yield await transition
        except StopAsyncIteration:
            return
        async for elm in iterable:
            yield elm

    return true_iterator(), remainder_iterator()
