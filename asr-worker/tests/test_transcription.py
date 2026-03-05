import logging
import os
import traceback

import pytest
from asr_worker.objects import (
    ASRPipelineConfig,
    ASRRequest,
    PreprocessingConfig,
    TaskQueues,
)
from asr_worker.workflows import ASRWorkflow
from datashare_python.types_ import TemporalClient
from temporalio.client import WorkflowFailureError
from temporalio.worker import Worker

logger = logging.getLogger(__name__)


@pytest.mark.e2e
async def test_asr_transcription(
    test_temporal_client_session: TemporalClient,  # noqa: ARG001
    cpu_worker: Worker,  # noqa: ARG001
    gpu_worker: Worker,  # noqa: ARG001
) -> None:
    audio_dir_path = os.path.join(os.getcwd(), "tests/resources/files/asr")
    audio_dir = os.environ.get("AUDIO_DIR", audio_dir_path)
    audio_path = os.path.join(audio_dir, "asr_test.wav")

    req: ASRRequest = ASRRequest(
        file_paths=[audio_path] * 5,
        pipeline=ASRPipelineConfig(preprocessing=PreprocessingConfig(batch_size=4)),
    )

    try:
        res = await test_temporal_client_session.execute_workflow(
            ASRWorkflow.run,
            req,
            id="test-001",
            task_queue=TaskQueues.CPU,
        )

        for transcription in res.transcriptions:
            assert transcription["score"] == -248.3
            assert transcription["transcription"] == [
                [0.08, 2.56, "To embrace the chaos that they fought in this battle."]
            ]

    except WorkflowFailureError:
        pytest.fail(traceback.format_exc())
