from typing import cast

from datashare_python.dependencies import lifespan_worker_config
from datashare_python.utils import WorkflowWithProgress
from temporalio import workflow

from .activities import NERActivity
from .config_ import NERWorkerConfig

with workflow.unsafe.imports_passed_through():
    from .objects_ import NERPayload, SpacyConfig, TaskQueues


# TODO: this workflow is meant to be compatible with
#  datashare-app/src/main/java/org/icij/datashare/tasks/BatchNlpTask.java ideally
#  we should reimplement from scratch in pure python


@workflow.defn(name="org.icij.datashare.tasks.BatchNlpTask")
class BatchNERWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: NERPayload) -> int:
        worker_config = cast(NERWorkerConfig, lifespan_worker_config())
        match args.config:
            case SpacyConfig():
                args, spacy_worker_config = _parse_spacy_args(args, worker_config)
                return await workflow.execute_activity(
                    NERActivity.spacy_ner,
                    args=args,
                    task_queue=TaskQueues.CPU,
                    start_to_close_timeout=spacy_worker_config.timeout,
                )
            case _:
                raise TypeError(f"unknown NER config type for {args.config}")


def _parse_spacy_args(args: NERPayload, worker_config: NERWorkerConfig) -> list:
    spacy_worker_config = worker_config.spacy
    args = [
        args.docs,
        args.categories,
        args.config,
        spacy_worker_config.max_length,
        spacy_worker_config.max_processes,
        spacy_worker_config.named_entity_buffer_size,
        spacy_worker_config.pipeline_batch_size,
    ]
    return args
