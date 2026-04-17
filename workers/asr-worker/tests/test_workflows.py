import json
import math
import shutil
import uuid
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from asr_worker.activities import ASRActivities
from asr_worker.config import ASRWorkerConfig
from asr_worker.constants import SUPPORTED_CONTENT_TYPES
from asr_worker.dependencies import REGISTRY
from asr_worker.objects import (
    ASRPipelineConfig,
    Timestamp,
    Transcript,
    Transcription,
)
from asr_worker.objects import ASRArgs, DocId
from asr_worker.workflows import ASRWorkflow, TaskQueues
from caul.objects import ASRResult
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document
from datashare_python.types_ import TemporalClient
from datashare_python.worker import worker_context
from pydantic import TypeAdapter
from temporalio.worker import Worker

from . import AUDIOS_PATH

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])

_MODEL_RESULT_0 = ASRResult(
    transcription=[(0.0, 2.0, "segment zero")], score=math.log(0.5)
)
_MODEL_RESULT_1 = ASRResult(
    transcription=[(0.0, 1.0, "segment one")], score=math.log(0.5)
)


_MODEL_RESULTS = [_MODEL_RESULT_0, _MODEL_RESULT_1]
_TRANSCRIPTIONS = [Transcription.from_asr_handler_result(res) for res in _MODEL_RESULTS]


@pytest.fixture
async def io_bound_worker(
    test_temporal_client_session: TemporalClient,
    test_worker_config: ASRWorkerConfig,
    event_loop: AbstractEventLoop,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"worker-{uuid.uuid4()}"
    task_queue = TaskQueues.IO
    dependencies = REGISTRY["io"]
    activities = ASRActivities(client, event_loop)
    worker_ctx = worker_context(
        worker_id,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        workflows=[ASRWorkflow],
        activities=[activities.search_audio_paths],
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture
async def cpu_bound_worker(
    test_worker_config: ASRWorkerConfig,  # noqa: ARG001
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    activities = ASRActivities(client, event_loop)
    worker_id = f"worker-{uuid.uuid4()}"
    task_queue = TaskQueues.CPU
    dependencies = REGISTRY["preprocessing"]
    worker_ctx = worker_context(
        worker_id,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        activities=[activities.preprocess, activities.postprocess],
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


@pytest.fixture
async def gpu_inference_worker(
    test_worker_config: ASRWorkerConfig,  # noqa: ARG001
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    activities = ASRActivities(client, event_loop)
    worker_id = f"worker-{uuid.uuid4()}"
    task_queue = TaskQueues.INFERENCE_GPU
    dependencies = REGISTRY["inference"]
    worker_ctx = worker_context(
        worker_id,
        worker_config=test_worker_config,
        client=client,
        event_loop=event_loop,
        task_queue=task_queue,
        activities=[activities.infer],
        dependencies=dependencies,
    )
    async with worker_ctx:
        yield


_EXPECTED_TRANSCRIPTION_0 = Transcription(
    transcripts=[
        Transcript(text="segment zero", timestamp=Timestamp(start_s=0.0, end_s=2.0)),
        Transcript(text="segment one", timestamp=Timestamp(start_s=0.0, end_s=1.0)),
        Transcript(text="segment zero", timestamp=Timestamp(start_s=0.0, end_s=2.0)),
        Transcript(text="segment one", timestamp=Timestamp(start_s=0.0, end_s=1.0)),
    ],
    confidence=0.5,
)
_EXPECTED_TRANSCRIPTION_1 = Transcription(
    transcripts=[
        Transcript(text="segment zero", timestamp=Timestamp(start_s=0.0, end_s=2.0)),
        Transcript(text="segment one", timestamp=Timestamp(start_s=0.0, end_s=1.0)),
    ],
    confidence=0.5,
)


@pytest.fixture
def with_audio_docs(
    populate_es_with_audio: list[Document], test_worker_config: ASRWorkerConfig
) -> list[tuple[DocId, Path]]:
    config = test_worker_config
    docs = [
        d for d in populate_es_with_audio if d.content_type in SUPPORTED_CONTENT_TYPES
    ]
    paths = []
    config.audios_root.mkdir(parents=True, exist_ok=True)
    audio_path = AUDIOS_PATH / "asr_test.wav"
    for doc in docs:
        shutil.copy(audio_path, config.audios_root / doc.path)
        paths.append((doc.id, doc.path))
    return paths


@pytest.mark.e2e
async def test_asr_workflow_e2e(
    test_temporal_client_session: TemporalClient,
    cpu_bound_worker: Worker,  # noqa: ARG001
    gpu_inference_worker: Worker,  # noqa: ARG001
    io_bound_worker: Worker,  # noqa: ARG001
    test_worker_config: ASRWorkerConfig,
    with_audio_docs: list[tuple[DocId, Path]],  # noqa: ARG001
) -> None:
    # Given
    config = test_worker_config
    artifacts_root = config.artifacts_root
    client = test_temporal_client_session
    n_audios = len(with_audio_docs)
    batch_size = n_audios - 1
    project = TEST_PROJECT
    doc_ids, _ = zip(*with_audio_docs, strict=True)
    doc_ids = list(doc_ids)
    args = ASRArgs(
        project=project, docs=doc_ids, config=ASRPipelineConfig(), batch_size=batch_size
    )
    workflow_id = f"asr-{uuid.uuid4().hex}"

    # When
    response = await client.execute_workflow(
        ASRWorkflow.run, args, id=workflow_id, task_queue=TaskQueues.IO
    )

    # Then
    assert response.n_transcribed == n_audios
    expected_artifact_dirs = [
        artifacts_root / project / "do" / "c-" / "doc-0",
        artifacts_root / project / "do" / "c-" / "doc-2",
    ]
    for d in expected_artifact_dirs:
        assert d.exists()
        assert d.is_dir()
        meta_path = d / "metadata.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        transcription_name = meta.get("transcription")
        assert transcription_name is not None
        transcription_path = d / transcription_name
        assert transcription_path.exists()
        transcription = Transcription.model_validate_json(
            transcription_path.read_text()
        )
        expcted_transcription = Transcription(
            transcripts=[
                Transcript(
                    text="To embrace the chaos that they fought in this battle.",
                    timestamp=Timestamp.from_floats(0.08, 2.56),
                )
            ],
            confidence=math.exp(-248.3),
        )
        assert transcription == expcted_transcription
