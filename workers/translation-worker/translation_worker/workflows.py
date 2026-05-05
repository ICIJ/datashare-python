import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import WorkflowWithProgress, execute_activity

    from .activities import TranslationActivities
    from .constants import TRANSLATION_WORKFLOW_NAME, TaskQueue
    from .objects import TranslationArgs, TranslationResponse


@workflow.defn(name=TRANSLATION_WORKFLOW_NAME)
class TranslationWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: TranslationArgs) -> TranslationResponse:
        # Create translation batches
        target_language = args.target_language.alpha2
        translation_batch_args = [args.project, target_language]
        translation_batches: list[tuple[str, list[list[str]]]]
        translation_batches = await execute_activity(
            TranslationActivities.create_translation_batches,
            args=translation_batch_args,
            task_queue=TaskQueue.IO,
            start_to_close_timeout=timedelta(hours=1),
        )

        # Translate
        translation_args = [
            (id_batch, target_language, args.project)
            for id_batch in translation_batches
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
