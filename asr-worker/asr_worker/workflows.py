from asyncio import gather
from datetime import timedelta
from enum import StrEnum
from itertools import repeat

from datashare_python.utils import WorkflowWithProgress
from pydantic import TypeAdapter
from temporalio import workflow

from asr_worker.constants import ASR_WORKFLOW, TEN_MINUTES
from asr_worker.models import ASRInputs, ASRResponse

with workflow.unsafe.imports_passed_through():
    from asr_worker.activities import ASRActivities

_ASR_INPUTS_TYPE_ADAPTER = TypeAdapter(ASRInputs)


class TaskQueues(StrEnum):
    IO = "asr.preprocessing.io"
    CPU = "asr.processing.cpu"
    INFERENCE_GPU = "asr.inference.gpu"
    INFERENCE_CPU = "asr.inference.cpu"


@workflow.defn(name=ASR_WORKFLOW)  # noqa: F821
class ASRWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, inputs: ASRInputs) -> ASRResponse:
        # Preprocessing
        logger = workflow.logger
        config = inputs.config
        batch_size = config.batch_size
        batched_input_paths = (
            inputs.paths[batch_start : batch_start + batch_size]
            for batch_start in range(0, len(inputs.paths), batch_size)
        )
        preprocess_args = zip(
            batched_input_paths, repeat(config.preprocessing), strict=False
        )
        preprocessing_acts = (
            workflow.execute_activity(
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
            preprocessed_batches, repeat(config.inference), strict=False
        )
        logger.info("preprocessing complete !")
        # Inference
        inference_acts = [
            workflow.execute_activity(
                ASRActivities.infer,
                b,
                # TODO: in practice we should parse the config to find out
                task_queue=TaskQueues.INFERENCE_CPU,
                start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
            )
            for b in preprocessed_batches
        ]
        logger.info("running inference...")
        inference_results = await gather(*inference_acts)
        logger.info("inference complete !")
        # Postprocessing
        postprocessing_ins = zip(
            inference_results,
            batched_input_paths,
            repeat(config.postprocessing),
            repeat(inputs.project),
            strict=False,
        )
        postprocessing_acts = [
            workflow.execute_activity(
                ASRActivities.postprocess,
                args=i,
                start_to_close_timeout=timedelta(seconds=TEN_MINUTES),
                task_queue=TaskQueues.CPU,
            )
            for i in postprocessing_ins
        ]
        logger.info("running postprocessing...")
        await gather(*postprocessing_acts)
        logger.info("postprocessing complete !")
        return ASRResponse(n_transcribed=len(inputs.paths))


REGISTRY = [ASRWorkflow]
