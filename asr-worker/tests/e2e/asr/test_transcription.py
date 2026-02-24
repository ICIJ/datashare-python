import asyncio
import os
import traceback

from temporalio.client import Client, WorkflowFailureError

from asr_worker.constants import ASR_TASK_QUEUE
from asr_worker.models import (
    ASRInputs,
    ASRPipelineConfig,
    PreprocessingConfig,
)
from asr_worker.workflow import ASRWorkflow


async def test_worker_transcription() -> None:
    # Create client connected to server at the given address
    client: Client = await Client.connect("localhost:7233")

    audio_dir = os.environ.get("AUDIO_DIR", "resources/files/asr")
    audio_path = os.path.join(audio_dir, "asr_test.wav")

    inputs: ASRInputs = ASRInputs(
        file_paths=[audio_path] * 5,
        pipeline=ASRPipelineConfig(preprocessing=PreprocessingConfig(batch_size=4)),
    )

    try:
        res = await client.execute_workflow(
            ASRWorkflow.run,
            inputs,
            id="test-001",
            task_queue=ASR_TASK_QUEUE,
        )

        for transcription in res.transcriptions:
            assert transcription["score"] == -248.3
            assert transcription["transcription"] == [
                [0.08, 2.56, "To embrace the chaos that they fought in this battle."]
            ]

    except WorkflowFailureError:
        print("Got expected exception: ", traceback.format_exc())
        assert False


if __name__ == "__main__":
    asyncio.run(test_worker_transcription())
