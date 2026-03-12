import asyncio
from datetime import timedelta
from typing import Any

from datashare_python.objects import WorkerResponseStatus
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import (
        WorkflowWithProgress,
    )

    from .activities import CreateTranslationBatches, TranslateDocs
    from .constants import TRANSLATION_WORKFLOW_NAME
    from .objects import TaskQueues, TranslationRequest, TranslationResponse


@workflow.defn(name=TRANSLATION_WORKFLOW_NAME)
class TranslationWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, payload: TranslationRequest) -> TranslationResponse:
        try:
            codes = await workflow.execute_activity(
                CreateTranslationBatches.language_alpha_codes,
                args=[payload.target_language],
                task_queue=TaskQueues.CPU,
                start_to_close_timeout=timedelta(minutes=2),
            )

            target_language_alpha_code = codes[0]

            translation_batch_args = [
                payload.project,
                target_language_alpha_code,
                payload.translation_config,
            ]
            # Create translation batches
            translation_batches: list[dict[str, Any]] = await workflow.execute_activity(
                CreateTranslationBatches.create_translation_batches,
                args=translation_batch_args,
                task_queue=TaskQueues.CPU,
                start_to_close_timeout=timedelta(hours=1),
            )
            # Translate
            translation_args = [
                (
                    b,
                    target_language_alpha_code,
                    payload.project,
                    payload.translation_config,
                )
                for b in translation_batches
            ]
            translations_activities = [
                workflow.execute_activity(
                    TranslateDocs.translate_sentences,
                    args=args,
                    task_queue=TaskQueues.GPU,
                    start_to_close_timeout=timedelta(hours=1),
                )
                for args in translation_args
            ]
            translations = await asyncio.gather(*translations_activities)
            num_translations = sum(translations)

            return TranslationResponse(
                status=WorkerResponseStatus.SUCCESS,
                num_translations=num_translations,
            )
        except ValueError as e:
            workflow.logger.exception(e)
            return TranslationResponse(status=WorkerResponseStatus.ERROR, error=str(e))


WORKFLOWS = [TranslationWorkflow]
