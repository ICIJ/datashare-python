from typing import AsyncGenerator, AsyncIterable, AsyncIterator

from aiostream.stream import chain

from datashare_python.utils import positional_args_only, before_and_after, once
from temporalio import activity


@positional_args_only
def hello_world_keyword(*, who: str) -> str:
    return f"hello {who}"


def test_keyword_safe_activity() -> None:
    # When
    try:
        activity.defn(hello_world_keyword)
    except Exception as e:
        raise AssertionError(
            "couldn't create activity from keyword only function "
        ) from e


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
