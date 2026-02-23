from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator

from aiostream.stream import chain
from utils_ import before_and_after, once


async def _num_gen() -> AsyncGenerator[int, None]:
    for i in range(10):
        yield i // 3


async def test_before_and_after() -> None:
    # Given
    async def group_by_iterator(
        items: AsyncIterable[int],
    ) -> AsyncIterator[AsyncIterator[int]]:
        while True:
            try:
                next_item = await anext(aiter(items))
            except StopAsyncIteration:
                return
            gr, items = before_and_after(items, lambda x, next_i=next_item: x == next_i)
            yield chain(once(next_item), gr)

    # When
    grouped = []
    async for group in group_by_iterator(_num_gen()):
        group = [item async for item in group]  # noqa: PLW2901
        grouped.append(group)
    assert grouped == [[0, 0, 0], [1, 1, 1], [2, 2, 2], [3]]
