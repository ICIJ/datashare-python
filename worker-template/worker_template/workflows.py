import asyncio
import logging
from datetime import timedelta
from enum import StrEnum

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import WorkflowWithProgress, execute_activity

    from .activities import (
        ClassifyDocs,
        CreateClassificationBatches,
        CreateTranslationBatches,
        Pong,
        TranslateDocs,
    )
    from .objects_ import TranslateAndClassifyArgs, TranslateAndClassifyResponse

logger = logging.getLogger(__name__)


class TaskQueues(StrEnum):
    IO = "worker-template.io"
    TRANSLATE_GPU = "worker-template.translate-gpu"
    CLASSIFY_GPU = "worker-template.classify-gpu"


@workflow.defn(name="translate-and-classify")
class TranslateAndClassifyWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: TranslateAndClassifyArgs) -> TranslateAndClassifyResponse:
        logger.info("creating translation batches...")
        translation_batch_args = [
            args.project,
            args.language,
            args.config.translation.batch_size,
        ]
        # Create translation batches
        heartbeat_timeout = timedelta(seconds=10)
        translation_batches = await execute_activity(
            CreateTranslationBatches.create_translation_batches,
            args=translation_batch_args,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=heartbeat_timeout,
        )
        # Translate
        logger.info("translating...")
        translation_args = [
            (b, args.language, args.project, args.config.translation)
            for b in translation_batches
        ]
        translations_activities = [
            execute_activity(
                TranslateDocs.translate_docs,
                args=args,
                task_queue=TaskQueues.TRANSLATE_GPU,
                start_to_close_timeout=timedelta(hours=1),
                heartbeat_timeout=heartbeat_timeout,
            )
            for args in translation_args
        ]
        translated = await asyncio.gather(*translations_activities)
        translated = sum(translated)
        # Create classification batches
        logger.info("creating classification batches...")
        clf_batch_args = [
            args.project,
            args.language,
            args.config.classification,
        ]
        clf_batches = await execute_activity(
            CreateClassificationBatches.create_classification_batches,
            args=clf_batch_args,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(days=1),
            heartbeat_timeout=heartbeat_timeout,
        )
        # Classify
        logger.info("classifying...")
        clf_args = [
            (b, args.language, args.project, args.config.classification)
            for b in clf_batches
        ]
        clf_activities = [
            execute_activity(
                ClassifyDocs.classify_docs,
                args=args,
                task_queue=TaskQueues.CLASSIFY_GPU,
                start_to_close_timeout=timedelta(days=1),
                heartbeat_timeout=heartbeat_timeout,
            )
            for args in clf_args
        ]
        classified = await asyncio.gather(*clf_activities)
        classified = sum(classified)
        logger.info("done !")
        return TranslateAndClassifyResponse(
            translated=translated, classified=classified
        )


@workflow.defn(name="ping")
class PingWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, arg: dict) -> str:  # noqa: ARG002
        logger.info("pinging !")
        return await execute_activity(
            Pong.pong,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(hours=1),
        )


WORKFLOWS = [TranslateAndClassifyWorkflow, PingWorkflow]
