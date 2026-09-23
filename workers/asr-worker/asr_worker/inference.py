import asyncio
import logging
from asyncio import AbstractEventLoop
from collections.abc import AsyncIterable, Iterable
from pathlib import Path
from typing import cast

from caul_core import ASRResult, FSProcessedSegment, InferenceRunner, SegmentIndex
from caul_core import Error as CaulError
from datashare_python.types_ import RawAsyncProgressHandler

logger = logging.getLogger(__name__)


async def infer_act(
    inference_runner: InferenceRunner,
    batches: Iterable[tuple[FSProcessedSegment, ...]],  # noqa: F821
    output_dir: Path,
    event_loop: AbstractEventLoop | None = None,
    progress: RawAsyncProgressHandler | None = None,
) -> tuple[list[Path], list[CaulError]]:
    # TODO: implement caching
    success = []
    errors = []
    inference_results = _non_blocking_inference(inference_runner, batches)
    res_i = 0
    async for asr_res in inference_results:
        if isinstance(asr_res, CaulError):
            errors.append(asr_res)
        else:
            index = asr_res.index
            res_path = output_dir / _transcript_file_name(index)
            res_path.parent.mkdir(parents=True, exist_ok=True)
            res_path.write_text(asr_res.model_dump_json())
            res_i += 1
            success.append(res_path)
        if progress is not None and event_loop is not None:
            await progress(res_i)
    return success, errors


def _transcript_file_name(index: SegmentIndex) -> str:
    # TODO: since audios were preprocessed on different workers (audio_idx, segment_idx)
    #  we should add the segment uuid to help debug. This will require a caul update
    # return f"{meta.index.audio}-{meta.index.segment}-{meta.uuid}-transcript.json"
    return f"{index.audio}-{index.segment}-transcript.json"


async def _non_blocking_inference(
    inference_runner: InferenceRunner, batches: Iterable[tuple[FSProcessedSegment, ...]]
) -> AsyncIterable[ASRResult]:
    # TODO: update this part to handle inference errors when caul does output errors

    # tee could hurt memory when buffering but here we advance both iterators at the
    # same pace so the tee iterator never buffers anything
    sentinel = object()
    results = iter(inference_runner.process(batches))
    while True:
        res = await asyncio.to_thread(next, results, sentinel)
        res = cast(ASRResult, res)
        if res is sentinel:
            break
        yield res
