from asyncio import gather
from dataclasses import asdict
from datetime import timedelta

from more_itertools import flatten
from temporalio import workflow

from asr_worker.constants import _TEN_MINUTES, RESPONSE_ERROR, RESPONSE_SUCCESS
from asr_worker.models import ASRInputs, ASRResponse

with workflow.unsafe.imports_passed_through():
    from asr_worker.activities import ASRActivities


# TODO: Figure out which modules are violating sandbox restrictions
#  and grant a limited passthrough
@workflow.defn(name="asr.transcription", sandboxed=False)
class ASRWorkflow:
    """ASR workflow definition"""

    def __init__(self):
        pass

    @workflow.run
    async def run(self, inputs: ASRInputs) -> ASRResponse:
        """Run ASR workflow

        :param inputs: ASRInputs
        :return: ASRResponse
        """
        try:
            # Preprocessing
            preprocessed_batches = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.preprocess,
                        inputs.file_paths[
                            offset : offset + inputs.pipeline.preprocessing.batch_size
                        ],
                        start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
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
                            inner_batch,
                            start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
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
                        flatten(inference_result_batch),
                        start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                    )
                    for inference_result_batch in inference_results
                ]
            )

            serialized_transcriptions = []

            # drop unnecessary fields, serialize
            for transcription in flatten(transcriptions):
                transcription = asdict(transcription)

                del transcription["input_ordering"]

                serialized_transcriptions.append(transcription)

            workflow.logger.info("Postprocessing complete")

            # TODO: Output formatting; do we want to keep PreprocessedInput metadata
            #  and remap results to it?
            return ASRResponse(
                status=RESPONSE_SUCCESS, transcriptions=serialized_transcriptions
            )
        except ValueError as e:
            workflow.logger.exception(e)
            return ASRResponse(status=RESPONSE_ERROR, error=str(e))


WORKFLOWS = [ASRWorkflow]
