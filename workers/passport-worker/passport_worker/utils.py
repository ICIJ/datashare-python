from collections.abc import AsyncIterable, Iterable
from pathlib import Path

from aiofile import async_open
from datashare_python.objects import ProcessedFile


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
