import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from typing import cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.config import ActivityTimeouts
    from datashare_python.utils import WorkflowWithProgress, execute_activity

    from .activities import PassportDetectionActivities
    from .config import PassportWorkerConfig
    from .objects import (
        Batches,
        PassportDetectionArgs,
        PassportDetectionResponse,
        PreprocessingBatches,
        PreprocessingConfig,
    )

logger = logging.getLogger(__name__)


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "passport-detection.io"
    PREPROCESSING = "passport-detection.preprocessing"
    INFERENCE = "passport-detection.inference"


@dataclass(frozen=True)
class PreprocessingOutput:
    pages: list[Path]
    errors: list[Path]


@workflow.defn(name="passport-detection.detect-passports")
class PassportDetectionWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: PassportDetectionArgs) -> PassportDetectionResponse:
        logger.info("creating preprocessing batches...")
        batch_args = [args.docs, args.project]
        # Fetch worker config
        worker_config = await execute_activity(
            PassportDetectionActivities.worker_config,
            args=batch_args,
            task_queue=TaskQueue.IO,
            timeouts=ActivityTimeouts(start_to_close=timedelta(minutes=5)),
        )
        worker_config = cast(PassportWorkerConfig, worker_config)
        # Create preprocessing batches
        preprocessing_batches = await execute_activity(
            PassportDetectionActivities.create_preprocessing_batches,
            args=batch_args,
            task_queue=TaskQueue.IO,
            timeouts=worker_config.timeouts.preprocessing_batching,
        )
        logger.info("created preprocessing batches!")
        # Preprocess
        preprocessing_output = await preprocess(
            args, preprocessing_batches, worker_config
        )
        # Create inference batches
        inference_batches = await execute_activity(
            PassportDetectionActivities.create_inference_batches,
            args=[preprocessing_output.pages, args.project],
            task_queue=TaskQueue.IO,
            timeouts=worker_config.timeouts.inference_batching,
        )
        # Perform inference
        logger.info("running inference...")
        inference_tasks = []
        for b in inference_batches:
            t = execute_activity(
                PassportDetectionActivities.detect_passports,
                args=(b, args),
                task_queue=TaskQueue.INFERENCE,
                timeouts=worker_config.timeouts.inference,
            )
            inference_tasks.append(t)
        inference_res = await asyncio.gather(*inference_tasks)
        logger.info("inference done !")
        logger.info("aggregating results...")
        # Aggregate stats and errors
        aggregation_args = [preprocessing_output.errors, inference_res]
        response = await execute_activity(
            PassportDetectionActivities.aggregate_results,
            args=aggregation_args,
            task_queue=TaskQueue.IO,
            timeouts=worker_config.timeouts.result_aggregation,
        )
        return response


async def preprocess(
    args: PassportDetectionArgs,
    preprocessing_batches: PreprocessingBatches,
    worker_config: PassportWorkerConfig,
) -> PreprocessingOutput:
    im_preprocessing_tasks = _im_processing_tasks(
        preprocessing_batches.images,
        args.project,
        args.config.preprocessing,
        worker_config,
    )
    force_reprocessing = not args.config.preprocessing.use_caching
    convert_to_pdf_tasks = _convert_to_pdfs_tasks(
        preprocessing_batches.to_pdf,
        args.project,
        force_reprocessing=force_reprocessing,
        worker_config=worker_config,
    )
    im_preprocessing_tasks = asyncio.gather(*im_preprocessing_tasks)
    convert_to_pdf_tasks = asyncio.gather(*convert_to_pdf_tasks)
    image_preprocessing_res, pdf_conversion_res = await asyncio.gather(
        im_preprocessing_tasks, convert_to_pdf_tasks
    )
    if image_preprocessing_res:
        im_pages_paths, im_preprocessing_errors = zip(
            *image_preprocessing_res, strict=True
        )
        im_pages_paths = list(im_pages_paths)
        im_preprocessing_errors = list(im_preprocessing_errors)
    else:
        im_pages_paths, im_preprocessing_errors = [], []

    if pdf_conversion_res:
        pdf_paths, pdf_conversion_errors = zip(*pdf_conversion_res, strict=True)
        pdf_paths = list(pdf_paths)
        pdf_conversion_errors = list(pdf_conversion_errors)
    else:
        pdf_paths, pdf_conversion_errors = [], []
    pdf_paths = list(pdf_paths)
    pdf_conversion_errors = list(pdf_conversion_errors)
    # Preprocess all files converted into PDFs + original PDFs
    logger.info("converting PDF pages to PNG...")
    pdf_batches = preprocessing_batches.pdfs + pdf_paths
    preprocess_pdfs_tasks = _process_pdfs_tasks(
        pdf_batches,
        args.project,
        force_reprocessing=force_reprocessing,
        worker_config=worker_config,
    )
    pdf_pages_res = await asyncio.gather(*preprocess_pdfs_tasks)
    if pdf_pages_res:
        pdfs_pages_paths, pdf_processing_errors = zip(*pdf_pages_res, strict=True)
        pdfs_pages_paths = list(pdfs_pages_paths)
        pdf_processing_errors = list(pdf_processing_errors)
    else:
        pdfs_pages_paths, pdf_processing_errors = [], []
    logger.info("done preprocessing !")
    all_pages = im_pages_paths + pdfs_pages_paths
    all_errors = im_preprocessing_errors + pdf_conversion_errors + pdf_processing_errors
    output = PreprocessingOutput(pages=all_pages, errors=all_errors)
    return output


def _im_processing_tasks(
    batches: Batches,
    project: str,
    config: PreprocessingConfig,
    worker_config: PassportWorkerConfig,
) -> list:
    im_preprocessing_tasks = []
    for b in batches:
        im_preprocessing_tasks.append(
            execute_activity(
                PassportDetectionActivities.preprocess_images,
                args=(b, project, config),
                task_queue=TaskQueue.PREPROCESSING,
                timeouts=worker_config.timeouts.image_preprocessing,
            )
        )
    return im_preprocessing_tasks


def _convert_to_pdfs_tasks(
    batches: Batches,
    project: str,
    *,
    force_reprocessing: bool,
    worker_config: PassportWorkerConfig,
) -> list[Coroutine]:
    all_tasks = []
    for b in batches:
        all_tasks.append(
            execute_activity(
                PassportDetectionActivities.convert_to_pdfs,
                args=(b, project, force_reprocessing),
                task_queue=TaskQueue.IO,
                timeouts=worker_config.timeouts.pdf_conversion,
            )
        )
    return all_tasks


def _process_pdfs_tasks(
    batches: Batches,
    project: str,
    *,
    force_reprocessing: bool,
    worker_config: PassportWorkerConfig,
) -> list[Coroutine]:
    all_tasks = []
    for b in batches:
        all_tasks.append(
            execute_activity(
                PassportDetectionActivities.preprocess_pdfs,
                args=(b, project, force_reprocessing),
                task_queue=TaskQueue.IO,
                timeouts=worker_config.timeouts.pdf_preprocessing,
            )
        )
    return all_tasks


WORKFLOWS = [PassportDetectionWorkflow]
