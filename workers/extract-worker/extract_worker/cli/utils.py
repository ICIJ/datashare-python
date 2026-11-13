import asyncio
import concurrent.futures
import sys
from collections.abc import Callable
from functools import wraps
from typing import Any

import typer


class AsyncTyper(typer.Typer):
    def async_command(self, *args, **kwargs) -> Callable[[Callable], Callable]:
        def decorator(async_func: Callable) -> Callable:
            @wraps(async_func)
            def sync_func(*_args, **_kwargs) -> Any:
                res = asyncio.run(async_func(*_args, **_kwargs))
                return res

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator


def eprint(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def _to_concurrent(
    fut: asyncio.Future, loop: asyncio.AbstractEventLoop
) -> concurrent.futures.Future:
    async def wait() -> None:
        await fut

    return asyncio.run_coroutine_threadsafe(wait(), loop)
