import logging
from asyncio import gather
from collections.abc import Iterable
from datetime import timedelta
from enum import StrEnum
from itertools import repeat
from pathlib import Path
from typing import Any

from caul_core import ASRPipelineConfig
from pydantic import TypeAdapter
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.config import ActivityTimeouts
    from datashare_python.utils import WorkflowWithProgress, execute_activity
    from icij_common.es import has_id

    from .activities import ASRActivities
    from .config import ASRWorkerConfig
    from .constants import ASR_WORKFLOW
    from .objects import ASRArgs, ASRResponse


_ASR_INPUTS_TYPE_ADAPTER = TypeAdapter(ASRArgs)

logger = logging.getLogger(__name__)


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
        worker_config = await _fetch_worker_config()
        batches = await _create_preprocessing_batches(args, worker_config)
        batches, preprocessing_errs, audio_routes = await _preprocess(
            args, batches, config, worker_config
        )
        asr_res, inference_errs = await _run_inference(batches, args, worker_config)
        doc_routes, postprocessing_errs = await _postprocess(
            asr_res, audio_routes, args, worker_config
        )
        _ = await _index_transcriptions(doc_routes, args, worker_config)
        flattened_batches = [f for b in batches for f in b]
        all_errors = preprocessing_errs + inference_errs + postprocessing_errs
        response = await _aggregate_results(
            flattened_batches,
            errors=all_errors,
            project=args.project,
            worker_config=worker_config,
        )
        return response


async def _fetch_worker_config() -> ASRWorkerConfig:
    worker_config = await execute_activity(
        ASRActivities.worker_config,
        timeouts=ActivityTimeouts(start_to_close=timedelta(minutes=2)),
        task_queue=TaskQueue.IO,
    )
    return worker_config


async def _create_preprocessing_batches(
    args: ASRArgs, worker_config: ASRWorkerConfig
) -> list[Path]:
    batch_size = args.batch_size
    doc_query = has_id(args.docs) if isinstance(args.docs, list) else args.docs
    search_args = [args.project, doc_query, batch_size]
    logger.info("searching files to process...")
    batch_paths = await execute_activity(
        ASRActivities.search_audio_paths,
        args=search_args,
        task_queue=TaskQueue.IO,
        timeouts=worker_config.timeouts.preprocessing,
    )
    return batch_paths


async def _preprocess(
    args: ASRArgs,
    batches: list[Any],
    config: ASRPipelineConfig,
    worker_config: ASRWorkerConfig,
) -> tuple[list[list[Path]], list[Path], list[Path]]:
    preprocess_args = zip(
        batches, repeat(args.project), repeat(config.preprocessing), strict=False
    )
    preprocessing_acts = (
        execute_activity(
            ASRActivities.preprocess,
            args=a,
            timeouts=worker_config.timeouts.preprocessing,
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
    batches: Iterable[list[Path]], args: ASRArgs, worker_config: ASRWorkerConfig
) -> tuple[list[list[Path]], list[Path]]:
    inference_args = zip(
        batches, repeat(args.project), repeat(args.config.inference), strict=False
    )
    inference_acts = [
        execute_activity(
            ASRActivities.infer,
            task_queue=TaskQueue.INFERENCE_GPU,
            args=b,
            timeouts=worker_config.timeouts.inference,
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
    inference_results: list[list[Path]],
    audio_routes: list[Path],
    args: ASRArgs,
    worker_config: ASRWorkerConfig,
) -> tuple[list[Path], list[Path]]:
    # strict zip make sure we're aligned
    inputs = zip(inference_results, audio_routes, strict=True)
    postprocessing_args = [(infer_res, routes, args) for infer_res, routes in inputs]
    postprocessing_acts = [
        execute_activity(
            ASRActivities.postprocess,
            args=i,
            timeouts=worker_config.timeouts.postprocessing,
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


async def _index_transcriptions(
    doc_routes: list[Path], args: ASRArgs, worker_config: ASRWorkerConfig
) -> int:
    indexing_args = list(
        zip(doc_routes, repeat(args.project), repeat(args.indexing), strict=False)
    )
    n_indexed = [
        execute_activity(
            ASRActivities.index_transcriptions,
            args=a,
            timeouts=worker_config.timeouts.inference,
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
    batches: list[Path],
    *,
    errors: list[Path],
    project: str,
    worker_config: ASRWorkerConfig,
) -> Any:
    aggregation_args = [batches, errors, project]
    response = await execute_activity(
        ASRActivities.aggregate_results,
        args=aggregation_args,
        task_queue=TaskQueue.IO,
        timeouts=worker_config.timeouts.aggregation,
    )
    return response


REGISTRY = [ASRWorkflow]
