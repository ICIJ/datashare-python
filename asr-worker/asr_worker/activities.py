import uuid
from pathlib import Path

from caul.model_handlers.objects import ASRModelHandlerResult
from caul.tasks.preprocessing.objects import PreprocessedInput
from config import ASRWorkerConfig
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    debuggable_name,
    read_artifact_metadata,
    safe_dir,
    to_raw_progress,
    write_artifact_metadata,
)
from datashare_python.utils import artifacts_dir as get_artifacts_dir
from pydantic import TypeAdapter
from temporalio import activity

from .constants import (
    POSTPROCESS_ACTIVITY,
    PREPROCESS_ACTIVITY,
    RUN_INFERENCE_ACTIVITY,
    TRANSCRIPTION_JSON,
    TRANSCRIPTION_METADATA_KEY,
)
from .models import Transcription

_BASE_WEIGHT = 1.0
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])


class ASRActivities(ActivityWithProgress):
    """Contains activity definitions as well as reference to models"""

    @activity_defn(name=PREPROCESS_ACTIVITY, progress_weight=_PREPROCESS_WEIGHT)
    def preprocess(self, paths: list[Path]) -> list[Path]:
        # TODO: this shouldn't be necessary, fix this bug
        paths = _LIST_OF_PATH_ADAPTER.validate_python(paths)
        worker_config = ASRWorkerConfig()
        audio_root = worker_config.audio_root
        workdir = worker_config.workdir
        # TODO: load from config passed at runtime with caching
        preprocessor = self.asr_handler.preprocessor
        # TODO: implement a caching strategy here, we could avoid processing files
        #  which have already been preprocessed
        to_process = [str(audio_root / p) for p in paths]
        batches = []
        # TODO: handle progress here
        for batch in preprocessor.process(str(to_process), output_dir=workdir):
            for preprocessed_input in batch:
                uuid_name = uuid.uuid4().hex[:20]
                segment_dir = safe_dir(uuid_name)
                # TODO: find a more debuggable name for this
                segment_path = workdir / segment_dir / f"{uuid_name}-preprocessed.json"
                segment_path.parent.mkdir(parents=True, exist_ok=True)
                preprocessed_input.model_dump_json(segment_path)
                batches.append(segment_path.relative_to(workdir))
        return batches

    @activity_defn(name=RUN_INFERENCE_ACTIVITY, progress_weight=_INFERENCE_WEIGHT)
    def infer(
        self,
        preprocessed_inputs: list[Path],
        *,
        progress: ProgressRateHandler | None = None,
    ) -> list[Path]:
        preprocessed_inputs = _LIST_OF_PATH_ADAPTER.validate_python(preprocessed_inputs)
        worker_config = ASRWorkerConfig()
        workdir = worker_config.workdir
        # TODO: load from config passed at runtime with caching
        inference_runner = self.asr_handler.inference_handler
        # TODO: extract this into a function to improve testability
        paths = []
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(preprocessed_inputs))
        abs_paths = [workdir / rel_path for rel_path in preprocessed_inputs]
        audios = (PreprocessedInput.model_validate_json(f) for f in abs_paths)
        for res_i, (path, asr_res) in enumerate(
            zip(preprocessed_inputs, inference_runner.process(audios), strict=True)
        ):
            filename = f"{debuggable_name(path)}-transcript.json"
            transcript_path = workdir / safe_dir(filename) / filename
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_text(asr_res.model_dump_json())
            paths.add(transcript_path.relative_to(workdir))
            if progress is not None:
                self._event_loop.run_until_complete(progress(res_i))
        return paths

    @activity_defn(name=POSTPROCESS_ACTIVITY, progress_weight=_BASE_WEIGHT)
    def postprocess(
        self,
        inference_results: list[Path],
        input_paths: list[Path],
        project: str,
        *,
        progress: ProgressRateHandler | None = None,
    ) -> None:
        inference_results = _LIST_OF_PATH_ADAPTER.validate_python(inference_results)
        input_paths = _LIST_OF_PATH_ADAPTER.validate_python(input_paths)
        worker_config = ASRWorkerConfig()
        artifacts_root = worker_config.artifact_root
        # TODO: load from config passed at runtime with caching
        post_processor = self.asr_handler.postprocess
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(input_paths))
        with post_processor:
            transcriptions = post_processor.process(inference_results)
            # Strict is important here !
            for i, (original, asr_result) in enumerate(
                zip(input_paths, transcriptions, strict=True)
            ):
                transcription_path = write_transcription(
                    asr_result,
                    original.name,
                    artifacts_root=artifacts_root,
                    project=project,
                )
                activity.logger.debug("wrote transcription for %s", transcription_path)
                if progress is not None:
                    self._event_loop.run_until_complete(progress(i))


def write_transcription(
    asr_result: ASRModelHandlerResult,
    transcribed_filename: str,
    *,
    artifacts_root: Path,
    project: str,
) -> Path:
    result = Transcription.from_asr_handler_result(asr_result)
    artifact_dir = artifacts_root / get_artifacts_dir(
        project, filename=transcribed_filename
    )
    artifact_dir.mkdir(exist_ok=True, parents=True)
    # TODO: if transcriptions are too large we could also serialize them
    #  as jsonl
    transcription_path = artifact_dir / TRANSCRIPTION_JSON
    transcription_path.write_text(result.model_dump_json())
    try:
        meta = read_artifact_metadata(
            artifacts_root, project, filename=transcribed_filename
        )
    except FileNotFoundError:
        meta = dict()
    meta[TRANSCRIPTION_METADATA_KEY] = transcription_path.name
    write_artifact_metadata(
        meta, artifacts_root, project=project, filename=transcribed_filename
    )
    return transcription_path


REGISTRY = [ASRActivities.preprocess, ASRActivities.infer, ASRActivities.postprocess]
