import asyncio
from datetime import timedelta
from enum import StrEnum

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import (
        ActivityWithProgress,
        WorkflowWithProgress,
        activity_defn,
        with_progress,
    )

    from .classify import ClassifyDocs, CreateClassificationBatches
    from .objects_ import TranslateAndClassifyRequest, TranslateAndClassifyResponse
    from .translate import CreateTranslationBatches, TranslateDocs


class TaskQueues(StrEnum):
    CPU = "cpu"
    TRANSLATE_GPU = "translate-gpu"
    CLASSIFY_GPU = "classify-gpu"


@workflow.defn(name="translate-and-classify")
class TranslateAndClassifyWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(
        self, payload: TranslateAndClassifyRequest
    ) -> TranslateAndClassifyResponse:
        translation_batch_args = [
            payload.project,
            payload.language,
            payload.config.translation.batch_size,
        ]
        # Create translation batches
        translation_batches = await workflow.execute_activity(
            CreateTranslationBatches.create_translation_batches,
            args=translation_batch_args,
            task_queue=TaskQueues.CPU,
            start_to_close_timeout=timedelta(hours=1),
        )
        # Translate
        translation_args = [
            (b, payload.language, payload.project, payload.config.translation)
            for b in translation_batches
        ]
        translations_activities = [
            workflow.execute_activity(
                TranslateDocs.translate_docs,
                args=args,
                task_queue=TaskQueues.TRANSLATE_GPU,
                start_to_close_timeout=timedelta(hours=1),
            )
            for args in translation_args
        ]
        translated = await asyncio.gather(*translations_activities)
        translated = sum(translated)
        # Create classification batches
        clf_batch_args = [
            payload.project,
            payload.language,
            payload.config.classification,
        ]
        clf_batches = await workflow.execute_activity(
            CreateClassificationBatches.create_classification_batches,
            args=clf_batch_args,
            task_queue=TaskQueues.CPU,
            start_to_close_timeout=timedelta(days=1),
        )
        # Classify
        clf_args = [
            (b, payload.language, payload.project, payload.config.classification)
            for b in clf_batches
        ]
        clf_activities = [
            workflow.execute_activity(
                ClassifyDocs.classify_docs,
                args=args,
                task_queue=TaskQueues.CLASSIFY_GPU,
                start_to_close_timeout=timedelta(days=1),
            )
            for args in clf_args
        ]
        classified = await asyncio.gather(*clf_activities)
        classified = sum(classified)
        return TranslateAndClassifyResponse(
            translated=translated, classified=classified
        )


@workflow.defn(name="ping")
class PingWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            Pong.pong,
            task_queue=TaskQueues.CPU,
            start_to_close_timeout=timedelta(hours=1),
        )


class Pong(ActivityWithProgress):
    @activity_defn(name="pong")
    @with_progress
    async def pong(self) -> str:
        return "pong"


WORKFLOWS = [TranslateAndClassifyWorkflow, PingWorkflow]
