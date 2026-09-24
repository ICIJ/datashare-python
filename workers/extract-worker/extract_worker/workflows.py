import asyncio
import logging
from datetime import timedelta
from enum import StrEnum

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.config import ActivityTimeouts
    from datashare_python.utils import WorkflowWithProgress, execute_activity

    from .activities import MarkdownExtract
    from .objects import MarkdownExtractArgs, MarkdownExtractResponse

logger = logging.getLogger(__name__)


class TaskQueues(StrEnum):
    EXTRACT_GPU = "extract.extract-gpu"
    EXTRACT_CPU = "extract.extract-cpu"
    IO = "extract.io"
    WORKFLOWS = "datashare.workflows"


@workflow.defn(name="extract.markdown")
class ExtractMarkdownContentWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: MarkdownExtractArgs) -> MarkdownExtractResponse:
        # Fetch worker config
        worker_config = await execute_activity(
            MarkdownExtract.extract_worker_config,
            task_queue=TaskQueues.IO,
            timeouts=ActivityTimeouts(start_to_close=timedelta(minutes=10)),
        )
        # Create batches almost of constant number of pages
        batch_args = [args.project, args.docs, args.config]
        logger.info("creating context extraction batches...")
        extract_batches = await execute_activity(
            MarkdownExtract.create_markdown_extract_batches,
            args=batch_args,
            task_queue=TaskQueues.IO,
            timeouts=worker_config.timeouts.create_batches,
        )

        # Extract Markdown content
        # Distribute batches docs with (more or less) constant number of page per batch,
        # across workers
        extract_args = [(b, args) for b in extract_batches]
        task_queue = worker_config.markdown.inference.device.md_extract_queue(
            args.config.pipeline
        )
        extract_acts = (
            execute_activity(
                MarkdownExtract.extract_markdown_content,
                args=args,
                task_queue=task_queue,
                timeouts=worker_config.timeouts.extract_content,
            )
            for args in extract_args
        )
        responses = await asyncio.gather(*extract_acts)
        return sum(responses, start=MarkdownExtractResponse())


WORKFLOWS = [ExtractMarkdownContentWorkflow]
