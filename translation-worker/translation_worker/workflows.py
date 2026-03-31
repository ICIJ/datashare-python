import asyncio
from datetime import timedelta

from datashare_python.objects import WorkerResponseStatus
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import (
        WorkflowWithProgress,
    )

    from .activities import (
        CreateTranslationBatches,
        TranslateDocs,
        resolve_language_alpha_code,
    )
    from .constants import TRANSLATION_WORKFLOW_NAME
    from .objects import (
        TaskQueues,
        TranslationRequest,
        TranslationResponse,
        TranslationWorkerConfig,
    )


@workflow.defn(name=TRANSLATION_WORKFLOW_NAME)
class TranslationWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, payload: TranslationRequest) -> TranslationResponse:
        worker_config = TranslationWorkerConfig()

        try:
            target_language_alpha_code = await workflow.execute_activity(
                resolve_language_alpha_code,
                payload.target_language,
                task_queue=TaskQueues.CPU,
                start_to_close_timeout=timedelta(seconds=30),
            )

            translation_batch_args = [
                payload.project,
                target_language_alpha_code,
                worker_config.max_batch_byte_len,
            ]
            # Create translation batches
            translation_batches: list[list[str]] = await workflow.execute_activity(
                CreateTranslationBatches.create_translation_batches,
                args=translation_batch_args,
                task_queue=TaskQueues.CPU,
                start_to_close_timeout=timedelta(hours=1),
            )
            # Translate
            translation_args = [
                (
                    id_batch,
                    target_language_alpha_code,
                    payload.project,
                    worker_config,
                )
                for id_batch in translation_batches
            ]
            translations_activities = [
                workflow.execute_activity(
                    TranslateDocs.translate_docs,
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
