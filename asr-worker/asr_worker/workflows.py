from asyncio import gather
from datetime import timedelta

from datashare_python.objects import WorkerResponseStatus
from more_itertools import flatten
from temporalio import workflow

from .objects import ASRRequest, ASRResponse, TaskQueues

with workflow.unsafe.imports_passed_through():
    from .activities import ASRActivities


# TODO: Figure out which modules are violating sandbox restrictions
#  and grant a limited passthrough
@workflow.defn(sandboxed=False)
class ASRWorkflow:
    """ASR workflow definition"""

    def __init__(self):
        pass

    @workflow.run
    async def run(self, inputs: ASRRequest) -> ASRResponse:
        """Run ASR workflow

        :param inputs: ASRRequest
        :return: ASRResponse
        """
        try:
            # Preprocessing
            preprocessed_batches = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.preprocess,
                        args=[
                            inputs.file_paths[
                                offset : offset
                                + inputs.pipeline.preprocessing.batch_size
                            ]
                        ],
                        task_queue=TaskQueues.CPU,
                        start_to_close_timeout=timedelta(minutes=10),
                    )
                    for offset in range(
                        0,
                        len(inputs.file_paths),
                        inputs.pipeline.preprocessing.batch_size,
                    )
                ]
            )

            workflow.logger.info("Preprocessing complete")

            # Inference
            inference_results = [
                await gather(
                    *[
                        workflow.execute_activity_method(
                            ASRActivities.infer,
                            args=[inner_batch],
                            task_queue=TaskQueues.GPU,
                            start_to_close_timeout=timedelta(minutes=10),
                        )
                        for inner_batch in outer_batch
                    ]
                )
                for outer_batch in preprocessed_batches
            ]

            workflow.logger.info("Inference complete")

            # Postprocessing
            transcriptions = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.postprocess,
                        args=[flatten(inference_result_batch)],
                        task_queue=TaskQueues.CPU,
                        start_to_close_timeout=timedelta(minutes=10),
                    )
                    for inference_result_batch in inference_results
                ]
            )

            serialized_transcriptions = []

            # drop unnecessary fields, serialize
            for trans in flatten(transcriptions):
                transcription = trans.model_dump()

                del transcription["input_ordering"]

                serialized_transcriptions.append(transcription)

            workflow.logger.info("Postprocessing complete")

            # TODO: Output formatting; do we want to keep PreprocessedInput metadata
            #  and remap results to it?
            return ASRResponse(
                status=WorkerResponseStatus.SUCCESS,
                transcriptions=serialized_transcriptions,
            )
        except ValueError as e:
            workflow.logger.exception(e)
            return ASRResponse(status=WorkerResponseStatus.ERROR, error=str(e))
