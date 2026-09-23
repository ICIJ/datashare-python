import logging
from asyncio import gather
from collections.abc import Iterable
from datetime import timedelta
from enum import StrEnum
from itertools import repeat
from pathlib import Path
from typing import Any

from caul_core import ASRPipelineConfig
from datashare_python.utils import WorkflowWithProgress, execute_activity
from icij_common.es import has_id
from pydantic import TypeAdapter
from temporalio import workflow

from .constants import ASR_WORKFLOW
from .objects import ASRArgs, ASRResponse

with workflow.unsafe.imports_passed_through():
    from .activities import ASRActivities

_ASR_INPUTS_TYPE_ADAPTER = TypeAdapter(ASRArgs)

logger = logging.getLogger(__name__)

_AUDIO_SEARCH_TIMEOUT = timedelta(minutes=10)
_INFERENCE_TIMEOUT = timedelta(minutes=30)
_INDEXATION_TIMEOUT = timedelta(hours=1)
_POSTPROCESSING_TIMEOUT = timedelta(minutes=10)
_RESULT_AGGREGATION_TIMEOUT = timedelta(minutes=5)


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "asr.io"
    CPU = "asr.cpu"
    INFERENCE_GPU = "asr.inference.gpu"
    INFERENCE_CPU = "asr.inference.cpu"


@workflow.defn(name=ASR_WORKFLOW)  # noqa: F821
class ASRWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: ASRArgs) -> ASRResponse:
        config = args.config
        batches = await _create_preprocessing_batches(args)
        batches, preprocessing_errs, audio_routes = await _preprocess(
            args, batches, config
        )
        asr_res, inference_errs = await _run_inference(batches, args)
        doc_routes, postprocessing_errs = await _postprocess(
            asr_res, audio_routes, args
        )
        _ = await _index_transcriptions(doc_routes, args)
        flattened_batches = [f for b in batches for f in b]
        all_errors = preprocessing_errs + inference_errs + postprocessing_errs
        response = await _aggregate_results(
            flattened_batches, errors=all_errors, project=args.project
        )
        return response


async def _create_preprocessing_batches(args: ASRArgs) -> list[Path]:
    batch_size = args.batch_size
    doc_query = has_id(args.docs) if isinstance(args.docs, list) else args.docs
    search_args = [args.project, doc_query, batch_size]
    logger.info("searching files to process...")
    batch_paths = await execute_activity(
        ASRActivities.search_audio_paths,
        args=search_args,
        start_to_close_timeout=_AUDIO_SEARCH_TIMEOUT,
        task_queue=TaskQueue.IO,
    )
    return batch_paths


async def _preprocess(
    args: ASRArgs, batches: list[Any], config: ASRPipelineConfig
) -> tuple[list[list[Path]], list[Path], list[Path]]:
    preprocess_args = zip(
        batches, repeat(args.project), repeat(config.preprocessing), strict=False
    )
    preprocessing_acts = (
        execute_activity(
            ASRActivities.preprocess,
            args=a,
            start_to_close_timeout=timedelta(minutes=10),
            task_queue=TaskQueue.CPU,
        )
        for a in preprocess_args
    )
    logger.info("preprocessing files...")
    preprocessing_res = await gather(*preprocessing_acts)
    batches, preprocessing_errors, audio_routes = zip(*preprocessing_res, strict=True)
    batches = list(batches)
    preprocessing_errors = list(preprocessing_errors)
    audio_routes = list(audio_routes)
    logger.info("preprocessing complete !")
    return batches, preprocessing_errors, audio_routes


async def _run_inference(
    batches: Iterable[list[Path]], args: ASRArgs
) -> tuple[list[list[Path]], list[Path]]:
    inference_args = zip(
        batches, repeat(args.project), repeat(args.config.inference), strict=False
    )
    inference_acts = [
        execute_activity(
            ASRActivities.infer,
            task_queue=TaskQueue.INFERENCE_GPU,
            args=b,
            # TODO: in practice we should parse the config to find out
            start_to_close_timeout=_INFERENCE_TIMEOUT,
            heartbeat_timeout=timedelta(minutes=3),
        )
        for b in inference_args
    ]
    logger.info("running inference...")
    inference_results = await gather(*inference_acts)
    success, errors = zip(*inference_results, strict=True)
    success = list(success)
    errors = list(errors)
    logger.info("inference complete !")
    return success, errors


async def _postprocess(
    inference_results: list[list[Path]], audio_routes: list[Path], args: ASRArgs
) -> tuple[list[Path], list[Path]]:
    # strict zip make sure we're aligned
    inputs = zip(inference_results, audio_routes, strict=True)
    postprocessing_args = [(infer_res, routes, args) for infer_res, routes in inputs]
    postprocessing_acts = [
        execute_activity(
            ASRActivities.postprocess,
            args=i,
            start_to_close_timeout=_POSTPROCESSING_TIMEOUT,
            task_queue=TaskQueue.CPU,
        )
        for i in postprocessing_args
    ]
    logger.info("running postprocessing...")
    postprocessing_res = await gather(*postprocessing_acts)
    logger.info("postprocessing complete !")
    routes, postprocessing_errors = zip(*postprocessing_res, strict=True)
    routes = list(routes)
    postprocessing_errors = list(postprocessing_errors)
    return routes, postprocessing_errors


async def _index_transcriptions(doc_routes: list[Path], args: ASRArgs) -> int:
    indexing_args = list(
        zip(doc_routes, repeat(args.project), repeat(args.indexing), strict=False)
    )
    n_indexed = [
        execute_activity(
            ASRActivities.index_transcriptions,
            args=a,
            start_to_close_timeout=_INDEXATION_TIMEOUT,
            task_queue=TaskQueue.IO,
        )
        for a in indexing_args
    ]
    logger.info("indexing transcriptions...")
    n_indexed = await gather(*n_indexed)
    logger.info("done indexing !")
    n_indexed = sum(n_indexed)
    return n_indexed


async def _aggregate_results(
    batches: list[Path], *, errors: list[Path], project: str
) -> Any:
    aggregation_args = [batches, errors, project]
    response = await execute_activity(
        ASRActivities.aggregate_results,
        args=aggregation_args,
        task_queue=TaskQueue.IO,
        start_to_close_timeout=_RESULT_AGGREGATION_TIMEOUT,
    )
    return response


REGISTRY = [ASRWorkflow]
