import asyncio
from datetime import timedelta
from enum import StrEnum

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import (
        WorkflowWithProgress,
    )

    from .activities import (
        ClassifyDocs,
        CreateClassificationBatches,
        CreateTranslationBatches,
        Pong,
        TranslateDocs,
    )
    from .objects_ import TranslateAndClassifyArgs, TranslateAndClassifyResponse


class TaskQueues(StrEnum):
    IO = "io"
    TRANSLATE_GPU = "translate-gpu"
    CLASSIFY_GPU = "classify-gpu"


@workflow.defn(name="translate-and-classify")
class TranslateAndClassifyWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: TranslateAndClassifyArgs) -> TranslateAndClassifyResponse:
        translation_batch_args = [
            args.project,
            args.language,
            args.config.translation.batch_size,
        ]
        # Create translation batches
        translation_batches = await workflow.execute_activity(
            CreateTranslationBatches.create_translation_batches,
            args=translation_batch_args,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(hours=1),
        )
        # Translate
        translation_args = [
            (b, args.language, args.project, args.config.translation)
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
            args.project,
            args.language,
            args.config.classification,
        ]
        clf_batches = await workflow.execute_activity(
            CreateClassificationBatches.create_classification_batches,
            args=clf_batch_args,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(days=1),
        )
        # Classify
        clf_args = [
            (b, args.language, args.project, args.config.classification)
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
    async def run(self, arg: dict) -> str:  # noqa: ARG002
        return await workflow.execute_activity(
            Pong.pong,
            task_queue=TaskQueues.IO,
            start_to_close_timeout=timedelta(hours=1),
        )


WORKFLOWS = [TranslateAndClassifyWorkflow, PingWorkflow]
