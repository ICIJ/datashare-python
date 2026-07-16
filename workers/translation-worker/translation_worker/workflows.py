import asyncio
from datetime import timedelta

from icij_common.iter_utils import batches
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import WorkflowWithProgress, execute_activity

    from .activities import TranslationActivities
    from .config import (
        TranslationWorkerConfig,
    )
    from .constants import TRANSLATION_WORKFLOW_NAME, TaskQueue
    from .objects import TranslationArgs, TranslationResponse


@workflow.defn(name=TRANSLATION_WORKFLOW_NAME)
class TranslationWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: TranslationArgs) -> TranslationResponse:
        # Get the config from the worker
        worker_config: TranslationWorkerConfig = await execute_activity(
            TranslationActivities.translation_worker_config,
            task_queue=TaskQueue.IO,
            start_to_close_timeout=timedelta(minutes=1),
        )
        batches_per_worker = worker_config.batches_per_worker
        # Create translation batches
        target = args.target_language
        translation_batch_args = [args.project, args.as_query()]
        per_language_batches: list[tuple[str, list[list[str]]]]
        per_language_batches = await execute_activity(
            TranslationActivities.create_translation_batches,
            args=translation_batch_args,
            task_queue=TaskQueue.IO,
            start_to_close_timeout=timedelta(hours=1),
        )

        # Translate
        translation_args = [
            (
                b,
                source,
                target,
                args.config,
                args.project,
            )
            for source, languages_batches in per_language_batches
            for b in batches(languages_batches, batch_size=batches_per_worker)
        ]
        translations_activities = (
            execute_activity(
                TranslationActivities.translate_docs,
                args=args,
                task_queue=TaskQueue.INFERENCE,
                start_to_close_timeout=timedelta(hours=1),
            )
            for args in translation_args
        )
        translations = await asyncio.gather(*translations_activities)
        num_translations = sum(translations)

        return TranslationResponse(n_translations=num_translations)


WORKFLOWS = [TranslationWorkflow]
