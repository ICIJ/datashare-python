import json
import math
import shutil
import uuid
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from asr_worker.activities import ASRActivities, write_transcription
from asr_worker.config import ASRWorkerConfig
from asr_worker.constants import (
    POSTPROCESS_ACTIVITY,
    PREPROCESS_ACTIVITY,
    RUN_INFERENCE_ACTIVITY,
)
from asr_worker.models import (
    ASRInputs,
    ASRPipelineConfig,
    ASRResponse,
    Timestamp,
    Transcript,
    Transcription,
)
from asr_worker.workflows import ASRWorkflow, TaskQueues
from caul.model_handlers.objects import ASRModelHandlerResult
from caul.tasks.preprocessing.objects import InputMetadata, PreprocessedInput
from datashare_python.conftest import TEST_PROJECT
from datashare_python.types_ import ProgressRateHandler, TemporalClient
from datashare_python.utils import ActivityWithProgress, activity_defn
from datashare_python.worker import datashare_worker
from pydantic import TypeAdapter
from temporalio import activity
from temporalio.worker import Worker

from . import AUDIOS_PATH

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])

_MODEL_RESULT_0 = ASRModelHandlerResult(
    transcription=[(0.0, 2.0, "segment zero")], score=math.log(0.5)
)
_MODEL_RESULT_1 = ASRModelHandlerResult(
    transcription=[(0.0, 1.0, "segment one")], score=math.log(0.5)
)


_MODEL_RESULTS = [_MODEL_RESULT_0, _MODEL_RESULT_1]
_TRANSCRIPTIONS = [Transcription.from_asr_handler_result(res) for res in _MODEL_RESULTS]


class MockedASRActivities(ActivityWithProgress):
    @activity_defn(name=PREPROCESS_ACTIVITY)
    def preprocess(self, paths: list[Path]) -> list[Path]:
        # TODO: this shouldn't be necessary, fix this bug
        paths = _LIST_OF_PATH_ADAPTER.validate_python(paths)
        workdir = ASRWorkerConfig().workdir
        workdir.mkdir(parents=True, exist_ok=True)
        batches = []
        for path_i, path in enumerate(paths):
            n_segments = len(path.name)
            for part_i in range(n_segments):
                seg_path = f"file_{path_i}_part_{part_i}.wav"
                metadata = InputMetadata(
                    duration=1,
                    input_ordering=path_i,
                    preprocessed_file_path=str(seg_path),
                )
                preprocessed_input = PreprocessedInput(metadata=metadata)
                (workdir / seg_path).write_text(preprocessed_input.model_dump_json())
                batches.append(seg_path)
        return batches

    @activity_defn(name=RUN_INFERENCE_ACTIVITY)
    def infer(
        self,
        preprocessed_inputs: list[Path],
        *,
        progress: ProgressRateHandler | None = None,  # noqa: ARG002
    ) -> list[Path]:  # noqa: ANN001, ARG001
        # TODO: this shouldn't be necessary, fix this bug
        preprocessed_inputs = _LIST_OF_PATH_ADAPTER.validate_python(preprocessed_inputs)
        workdir = ASRWorkerConfig().workdir
        paths = []
        preprocessed_inputs = [
            PreprocessedInput.model_validate_json((workdir / p).read_text())
            for p in preprocessed_inputs
        ]
        for preprocessed_i, i in enumerate(preprocessed_inputs):
            res = _MODEL_RESULTS[preprocessed_i % len(_MODEL_RESULTS)]
            res = ASRModelHandlerResult(
                input_ordering=i.metadata.input_ordering,
                transcription=res.transcription,
                score=res.score,
            )
            filename = f"{uuid.uuid4().hex[:20]}-transcript.json"
            (workdir / filename).write_text(res.model_dump_json())
            paths.append(filename)
        return paths

    @activity.defn(name=POSTPROCESS_ACTIVITY)
    def postprocess(
        self,
        inference_results: list[Path],
        input_paths: list[Path],
        project: str,
        progress: ProgressRateHandler | None = None,  # noqa: ARG002
    ) -> None:
        # TODO: this shouldn't be necessary, fix this bug
        inference_results = _LIST_OF_PATH_ADAPTER.validate_python(inference_results)
        input_paths = _LIST_OF_PATH_ADAPTER.validate_python(input_paths)
        config = ASRWorkerConfig()
        artifact_root = config.artifacts_root
        workdir = config.workdir
        artifact_root.mkdir(parents=True, exist_ok=True)
        inference_results = [
            ASRModelHandlerResult.model_validate_json((workdir / f).read_text())
            for f in inference_results
        ]
        current_res = None
        asr_results, transcription, scores = [], [], []
        for res in inference_results:
            if res.input_ordering != current_res and current_res is not None:
                score = (sum(scores) / len(scores)) if scores else 0
                asr_results.append(
                    ASRModelHandlerResult(
                        transcription=sum(transcription, []), score=score
                    )
                )
                asr_results, transcription, scores = [], [], []
                current_res = res.input_ordering
            transcription.append(res.transcription)
            scores.append(res.score)
        asr_results.append(
            ASRModelHandlerResult(
                transcription=sum(transcription, []), score=sum(scores) / len(scores)
            )
        )
        for original, asr_result in zip(input_paths, asr_results, strict=True):
            write_transcription(
                asr_result, original.name, artifacts_root=artifact_root, project=project
            )


@pytest.fixture
async def io_bound_worker(
    test_temporal_client_session: TemporalClient,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    client = test_temporal_client_session
    worker = datashare_worker(client, task_queue=TaskQueues.IO, workflows=[ASRWorkflow])
    async with worker:
        yield worker


@pytest.fixture
async def mock_cpu_bound_worker(
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    client = test_temporal_client_session
    activities = MockedASRActivities(client, event_loop)
    worker = datashare_worker(
        client,
        task_queue=TaskQueues.CPU,
        activities=[activities.preprocess, activities.postprocess],
    )
    async with worker:
        yield worker


@pytest.fixture
async def mock_cpu_inference_worker(
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    client = test_temporal_client_session
    activities = MockedASRActivities(client, event_loop)
    worker = datashare_worker(
        client, task_queue=TaskQueues.INFERENCE_CPU, activities=[activities.infer]
    )
    async with worker:
        yield worker


@pytest.fixture
async def cpu_bound_worker(
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    client = test_temporal_client_session
    activities = ASRActivities(client, event_loop)
    worker = datashare_worker(
        client,
        task_queue=TaskQueues.CPU,
        activities=[activities.preprocess, activities.postprocess],
    )
    async with worker:
        yield worker


@pytest.fixture
async def cpu_inference_worker(
    test_temporal_client_session: TemporalClient,
    event_loop: AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[Worker, None]:
    client = test_temporal_client_session
    activities = ASRActivities(client, event_loop)
    worker = datashare_worker(
        client, task_queue=TaskQueues.INFERENCE_CPU, activities=[activities.infer]
    )
    async with worker:
        yield worker


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


async def test_asr_workflow(
    test_temporal_client_session: TemporalClient,
    mock_cpu_bound_worker: Worker,  # noqa: ARG001
    mock_cpu_inference_worker: Worker,  # noqa: ARG001
    io_bound_worker: Worker,  # noqa: ARG001
    mocked_worker_config_in_env: ASRWorkerConfig,
) -> None:
    # Given
    worker_config = mocked_worker_config_in_env
    client = test_temporal_client_session
    path = [Path("aabb"), Path("cc")]
    batch_size = 1
    config = ASRPipelineConfig(batch_size=batch_size)
    workflow_id = f"asr-{uuid.uuid4().hex}"
    project = TEST_PROJECT
    inputs = ASRInputs(project=project, docs=path, config=config)
    # When
    result = await client.execute_workflow(
        ASRWorkflow.run, inputs, id=workflow_id, task_queue=TaskQueues.IO
    )
    # Then
    expected_response = ASRResponse(n_transcribed=2)
    assert result == expected_response
    artifacts_dir = worker_config.artifacts_root
    expected_transcription_dirs = [
        artifacts_dir / project / "aa" / "bb" / "aabb",
        artifacts_dir / project / "cc" / "cc",
    ]
    expected_transcriptions = [_EXPECTED_TRANSCRIPTION_0, _EXPECTED_TRANSCRIPTION_1]
    for expected_t, d in zip(
        expected_transcriptions, expected_transcription_dirs, strict=True
    ):
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
        assert transcription == expected_t


@pytest.fixture
def with_audios(mocked_worker_config_in_env: ASRWorkerConfig) -> list[Path]:
    config = mocked_worker_config_in_env
    audios = [f for f in AUDIOS_PATH.iterdir() if f.suffix == ".wav"]
    paths = []
    config.audios_root.mkdir(parents=True, exist_ok=True)
    for audio in audios:
        rel_path = audio.relative_to(AUDIOS_PATH)
        shutil.copy(audio, config.audios_root / rel_path)
        paths.append(rel_path)
    return paths


@pytest.mark.e2e
async def test_asr_workflow_e2e(
    test_temporal_client_session: TemporalClient,
    cpu_bound_worker: Worker,  # noqa: ARG001
    cpu_inference_worker: Worker,  # noqa: ARG001
    io_bound_worker: Worker,  # noqa: ARG001
    mocked_worker_config_in_env: ASRWorkerConfig,  # noqa: ARG001
    with_audios: list[Path],
) -> None:
    # Given
    config = mocked_worker_config_in_env
    client = test_temporal_client_session
    n_audios = 3
    batch_size = n_audios - 1
    audios = with_audios * n_audios
    inputs = ASRInputs(
        project=TEST_PROJECT,
        docs=audios,
        config=ASRPipelineConfig(batch_size=batch_size),
    )
    workflow_id = f"asr-{uuid.uuid4().hex}"

    # When
    response = await client.execute_workflow(
        ASRWorkflow.run, inputs, id=workflow_id, task_queue=TaskQueues.IO
    )

    # Then
    assert response.n_transcribed == n_audios
    expected_transcription_path = config.artifacts_root / "as" / "r_" / "asr_test"
    assert expected_transcription_path.exists()
    assert expected_transcription_path.is_dir()
    meta_path = expected_transcription_path / "metadata.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    transcription_name = meta.get("transcription")
    assert transcription_name is not None
    transcription_path = expected_transcription_path / transcription_name
    assert transcription_path.exists()
    transcription = Transcription.model_validate_json(transcription_path.read_text())
    expcted_transcription = Transcription(
        transcripts=[
            Transcript(
                text="To embrace the chaos that they fought in this battle.",
                timestamp=Timestamp.from_floats(0.08, 2.56),
            )
        ],
        confidence=math.log(-248.3),
    )
    assert transcription == expcted_transcription
