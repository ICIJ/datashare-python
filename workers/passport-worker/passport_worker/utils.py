import logging
from collections.abc import AsyncIterable, Callable, Iterable
from functools import wraps
from inspect import iscoroutinefunction
from pathlib import Path
from typing import Protocol

from aiofile import async_open
from datashare_python.objects import ProcessedFile

from passport_worker.objects import FileProcessingError

logger = logging.getLogger(__name__)


async def write_batches(
    batches: AsyncIterable[list[ProcessedFile]] | Iterable[list[ProcessedFile]],
    root: Path,
    batch_offset: int = 0,
    prefix: str = "batch_",
) -> AsyncIterable[Path]:
    if hasattr(batches, "__aiter__"):
        async for b in _async_write_batches(batches, root, batch_offset, prefix):
            yield b
        return
    async for b in _write_batches(batches, root, batch_offset, prefix):
        yield b


async def _async_write_batches(
    batches: AsyncIterable[list[ProcessedFile]],
    root: Path,
    batch_offset: int,
    prefix: str,
) -> AsyncIterable[Path]:
    batch_id = batch_offset
    async for batch in batches:
        batch_path = root / f"{batch_id // 1000}" / f"{prefix}{batch_id}.jsonl"
        batch_path.parent.mkdir(parents=True, exist_ok=True)
        async with async_open(batch_path, "w") as f:
            for fs_doc in batch:
                await f.write(f"{fs_doc.model_dump_json()}\n")
        yield batch_path
        batch_id += 1


async def _write_batches(
    batches: Iterable[list[ProcessedFile]],
    root: Path,
    batch_offset: int,
    prefix: str,
) -> AsyncIterable[Path]:
    batch_id = batch_offset
    for batch in batches:
        batch_path = root / f"{batch_id // 1000}" / f"{prefix}{batch_id}.jsonl"
        batch_path.parent.mkdir(parents=True, exist_ok=True)
        async with async_open(batch_path, "w") as f:
            for fs_doc in batch:
                await f.write(f"{fs_doc.model_dump_json()}\n")
        yield batch_path
        batch_id += 1


class _PreprocessingFunction[R](Protocol):
    def __call__(self, doc: ProcessedFile, *args, **kwargs) -> R: ...


def reports_errors[R](
    errors: tuple[type[Exception]],
) -> Callable[
    [_PreprocessingFunction[R]], _PreprocessingFunction[R | FileProcessingError]
]:

    def parent_wrapper(f) -> _PreprocessingFunction[R | FileProcessingError]:
        if iscoroutinefunction(f):

            @wraps(f)  # noqa: F821
            async def wrapper(
                doc: ProcessedFile, *args, **kwargs
            ) -> R | FileProcessingError:
                try:
                    return await f(doc, *args, **kwargs)
                except errors as e:
                    logger.exception("error while processing doc %s", doc)
                    report = FileProcessingError.from_exception(doc, e)
                    return report
        else:

            @wraps(f)
            def wrapper(doc: ProcessedFile, *args, **kwargs) -> R | FileProcessingError:

                try:
                    return f(doc, *args, **kwargs)
                except errors as e:
                    logger.exception("error while processing doc %s", doc)
                    report = FileProcessingError.from_exception(doc, e)
                    return report

        return wrapper

    return parent_wrapper
