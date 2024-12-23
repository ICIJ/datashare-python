import asyncio
import concurrent.futures
import sys
from functools import wraps

import typer


class AsyncTyper(typer.Typer):
    def async_command(self, *args, **kwargs):
        def decorator(async_func):
            @wraps(async_func)
            def sync_func(*_args, **_kwargs):
                res = asyncio.run(async_func(*_args, **_kwargs))
                return res

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def _to_concurrent(
    fut: asyncio.Future, loop: asyncio.AbstractEventLoop
) -> concurrent.futures.Future:
    async def wait():
        await fut

    return asyncio.run_coroutine_threadsafe(wait(), loop)
