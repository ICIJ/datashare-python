from pathlib import Path

from aiofile import async_open
from caul_core import Error as CaulError
from datashare_python.objects import Error, ErrorReport, ProcessingReport, WorkerRoots
from datashare_python.utils import async_read_jsonl_as

from asr_worker.objects import ASRResponse

from .objects import ASRError, ASRErrorSource


async def aggregate_results_act(
    batches: list[Path],
    errors: list[Path],
    project: str,
    roots: WorkerRoots,
) -> ASRResponse:
    workdir = roots.workdir
    n_processed = 0
    for b in batches:
        n_processed += await _count_lines(workdir / b)
    processed = ProcessingReport(n_docs=n_processed)
    errors = (
        err
        for p in errors
        async for err in async_read_jsonl_as(roots.workdir / p, CaulError)
    )
    errors = [_caul_to_ds_asr_error(err, project) async for err in errors]
    errors = ErrorReport.from_errors(*errors)
    success = ProcessingReport(n_docs=n_processed - len(errors.errors))
    res = ASRResponse(processed=processed, successes=success, errors=errors)
    return res


async def _count_lines(path: Path) -> int:
    n_lines = 0
    async with async_open(path) as f:
        async for _ in f:
            n_lines += 1
    return n_lines


def _caul_to_ds_asr_error(error: CaulError, project: str) -> ASRError:  # noqa: F821
    audio = error.metadata.index.audio
    if isinstance(audio, int):
        msg = "expected audios to be indexed by ID"
        raise TypeError(msg)
    source = ASRErrorSource(doc_id=audio, project=project)
    error = Error(title=error.title, detail=error.detail)
    return ASRError(source=source, error=error)
