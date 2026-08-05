import asyncio
from collections.abc import AsyncIterable
from pathlib import Path
from typing import TypeVar

from datashare_python.objects import WorkerPaths
from datashare_python.utils import async_read_jsonl_as

from passport_worker.objects import (
    FileProcessingError,
    PartialDetectionResult,
    PassportDetectionResponse,
)


async def aggregate_results_act(
    error_paths: list[Path], *, result_paths: list[Path], paths: WorkerPaths
) -> PassportDetectionResponse:
    workdir = paths.workdir
    preprocessing_errors = await asyncio.gather(
        *(
            _as_list(async_read_jsonl_as(workdir / p, FileProcessingError))
            for p in error_paths
        )
    )
    preprocessing_errors = sum(preprocessing_errors, [])
    inference_results = await asyncio.gather(
        *(
            _as_list(async_read_jsonl_as(workdir / p, PartialDetectionResult))
            for p in result_paths
        )
    )
    inference_results = sum(inference_results, [])
    res = PassportDetectionResponse.aggregate(
        preprocessing_errors, inference_results=inference_results
    )
    return res


T = TypeVar("T")


async def _as_list[T](iterable: AsyncIterable[T]) -> list[T]:
    return [i async for i in iterable]
