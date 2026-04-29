import logging
from asyncio import gather
from datetime import timedelta
from enum import StrEnum
from itertools import repeat

from datashare_python.utils import WorkflowWithProgress, execute_activity
from icij_common.es import has_id
from pydantic import TypeAdapter
from temporalio import workflow

from asr_worker.constants import ASR_WORKFLOW, TEN_MINUTES
from asr_worker.objects import ASRArgs, ASRResponse

with workflow.unsafe.imports_passed_through():
    from asr_worker.activities import ASRActivities

_ASR_INPUTS_TYPE_ADAPTER = TypeAdapter(ASRArgs)

logger = logging.getLogger(__name__)


class TaskQueues(StrEnum):
    IO = "asr.io"
    CPU = "asr.preprocessing.cpu"
    INFERENCE_GPU = "asr.inference.gpu"
    INFERENCE_CPU = "asr.inference.cpu"


@workflow.defn(name=ASR_WORKFLOW)  # noqa: F821
class ASRWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: ASRArgs) -> ASRResponse:
        config = args.config
        batch_size = args.batch_size
        doc_query = has_id(args.docs) if isinstance(args.docs, list) else args.docs
        search_args = [args.project, doc_query, batch_size]
        logger.info("searching files to process...")
        batch_paths = await execute_activity(
            ASRActivities.search_audio_paths,
            args=search_args,
            start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
            task_queue=TaskQueues.IO,
        )
        # Preprocessing
        preprocess_args = zip(batch_paths, repeat(config.preprocessing), strict=False)
        preprocessing_acts = (
            execute_activity(
                ASRActivities.preprocess,
                args=a,
                start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
                task_queue=TaskQueues.CPU,
            )
            for a in preprocess_args
        )
        logger.info("preprocessing files...")
        preprocessed_batches = await gather(*preprocessing_acts)
        inference_args = zip(
            preprocessed_batches,
            repeat(args.project),
            repeat(config.inference),
            strict=False,
        )
        logger.info("preprocessing complete !")
        # Inference
        inference_acts = [
            execute_activity(
                ASRActivities.infer,
                task_queue=TaskQueues.INFERENCE_GPU,
                args=b,
                # TODO: in practice we should parse the config to find out
                start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
            )
            for b in inference_args
        ]
        logger.info("running inference...")
        inference_results = await gather(*inference_acts)
        logger.info("inference complete !")
        # Postprocessing
        postprocessing_ins = list(
            zip(
                inference_results,
                batch_paths,
                repeat(config.postprocessing),
                repeat(args.project),
                strict=False,
            )
        )
        postprocessing_acts = [
            execute_activity(
                ASRActivities.postprocess,
                args=i,
                start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
                task_queue=TaskQueues.CPU,
            )
            for i in postprocessing_ins
        ]
        logger.info("running postprocessing...")
        n_transcribed = await gather(*postprocessing_acts)
        n_transcribed = sum(n_transcribed)
        logger.info("postprocessing complete !")
        return ASRResponse(n_transcribed=n_transcribed)


REGISTRY = [ASRWorkflow]
