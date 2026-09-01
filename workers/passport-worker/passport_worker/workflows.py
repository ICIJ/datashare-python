import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from pathlib import Path

from datashare_python.utils import WorkflowWithProgress, execute_activity
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from .activities import Activity, PassportDetectionActivities
    from .objects import (
        Batches,
        ImagePreprocessorConfig,
        PassportDetectionArgs,
        PassportDetectionResponse,
        PreprocessingBatches,
    )

logger = logging.getLogger(__name__)


class TaskQueue(StrEnum):
    WORKFLOWS = "datashare.workflows"
    IO = "passport-detection.io"
    PREPROCESSING = "passport-detection.preprocessing"
    INFERENCE = "passport-detection.inference"


_CREATE_BATCHES_TIMEOUT = timedelta(minutes=30)
_PREPROCESS_IMAGES_TIMEOUT = timedelta(hours=1)
_CONVERT_TO_PDF_TIMEOUT = timedelta(hours=1)
_PREPROCESS_PDF_TIMEOUT = timedelta(minutes=10)
_INFERENCE_TIMEOUT = timedelta(minutes=10)
_RESULT_AGGREGATION_TIMEOUT = timedelta(minutes=5)


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
        # Create preprocessing batches
        preprocessing_batches = await execute_activity(
            PassportDetectionActivities.create_preprocessing_batches,
            args=batch_args,
            task_queue=TaskQueue.IO,
            start_to_close_timeout=_CREATE_BATCHES_TIMEOUT,
        )
        logger.info("created preprocessing batches!")
        # Preprocess
        preprocessing_output = await preprocess(args, preprocessing_batches)
        # Create inference batches
        inference_batches = await execute_activity(
            Activity.CREATE_INFERENCE_BATCHES,
            args=[preprocessing_output.pages, args.project],
            task_queue=TaskQueue.IO,
            start_to_close_timeout=_CREATE_BATCHES_TIMEOUT,
        )
        # Perform inference
        logger.info("running inference...")
        inference_tasks = []
        for b in inference_batches:
            t = execute_activity(
                PassportDetectionActivities.detect_passports,
                args=(b, args),
                task_queue=TaskQueue.INFERENCE,
                start_to_close_timeout=_INFERENCE_TIMEOUT,
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
            start_to_close_timeout=_RESULT_AGGREGATION_TIMEOUT,
        )
        return response


async def preprocess(
    args: PassportDetectionArgs, preprocessing_batches: PreprocessingBatches
) -> PreprocessingOutput:
    im_preprocessing_tasks = _im_processing_tasks(
        preprocessing_batches.images, args.project, args.config.preprocessing.images
    )
    convert_to_pdf_tasks = _convert_to_pdfs_tasks(
        preprocessing_batches.to_pdf, args.project
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
    preprocess_pdfs_tasks = _process_pdfs_tasks(pdf_batches, args.project)
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
    batches: Batches, project: str, config: ImagePreprocessorConfig
) -> list:
    im_preprocessing_tasks = []
    for b in batches:
        im_preprocessing_tasks.append(
            execute_activity(
                PassportDetectionActivities.preprocess_images,
                args=(b, project, config),
                task_queue=TaskQueue.PREPROCESSING,
                start_to_close_timeout=_PREPROCESS_IMAGES_TIMEOUT,
                heartbeat_timeout=timedelta(minutes=3),
            )
        )
    return im_preprocessing_tasks


def _convert_to_pdfs_tasks(batches: Batches, project: str) -> list[Coroutine]:
    all_tasks = []
    for b in batches:
        all_tasks.append(
            execute_activity(
                PassportDetectionActivities.convert_to_pdfs,
                args=(b, project),
                task_queue=TaskQueue.IO,
                start_to_close_timeout=_CONVERT_TO_PDF_TIMEOUT,
                heartbeat_timeout=timedelta(minutes=2),
            )
        )
    return all_tasks


def _process_pdfs_tasks(batches: Batches, project: str) -> list[Coroutine]:
    all_tasks = []
    for b in batches:
        all_tasks.append(
            execute_activity(
                PassportDetectionActivities.preprocess_pdfs,
                args=(b, project),
                task_queue=TaskQueue.IO,
                start_to_close_timeout=_CONVERT_TO_PDF_TIMEOUT,
            )
        )
    return all_tasks


WORKFLOWS = [PassportDetectionWorkflow]
