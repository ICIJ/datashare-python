from collections.abc import Iterable
from itertools import tee
from pathlib import Path

from caul.objects import ASRResult, PreprocessedInput
from caul.tasks import (
    InferenceRunner,
    ParakeetPostprocessorConfig,
    ParakeetPreprocessorConfig,
    Postprocessor,
    Preprocessor,
)
from datashare_python.types_ import ProgressRateHandler
from datashare_python.utils import (
    ActivityWithProgress,
    activity_contextual_id,
    activity_defn,
    debuggable_name,
    safe_dir,
    to_raw_progress,
    write_artifact,
)
from icij_common.pydantic_utils import safe_copy
from pydantic import TypeAdapter
from temporalio import activity

from asr_worker.utils import read_jsonl

from .config import ASRWorkerConfig
from .constants import (
    POSTPROCESS_ACTIVITY,
    PREPROCESS_ACTIVITY,
    RUN_INFERENCE_ACTIVITY,
    TRANSCRIPTION_METADATA_KEY,
    TRANSCRIPTION_METADATA_VALUE,
)
from .models import InferenceRunnerConfig, Transcription

_BASE_WEIGHT = 1.0
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])
_INFERENCE_CONFIG_TYPE_ADAPTER = TypeAdapter(InferenceRunnerConfig)


class ASRActivities(ActivityWithProgress):
    @activity_defn(name=PREPROCESS_ACTIVITY, progress_weight=_PREPROCESS_WEIGHT)
    def preprocess(
        self, paths: list[Path], config: ParakeetPreprocessorConfig
    ) -> list[Path]:
        # TODO: this shouldn't be necessary, fix this bug
        paths = _LIST_OF_PATH_ADAPTER.validate_python(paths)
        worker_config = ASRWorkerConfig()
        audio_root = worker_config.audios_root
        workdir = worker_config.workdir
        audios = (str(audio_root / p) for p in paths)
        # TODO: implement caching
        preprocessor = Preprocessor.from_config(config)
        contextual_id = activity_contextual_id()
        output_dir = workdir / contextual_id
        output_dir.mkdir(parents=True, exist_ok=True)
        with preprocessor:
            # TODO: implement a caching strategy here, we could avoid processing files
            #  which have already been preprocessed
            batches = [
                f.relative_to(workdir)
                for f in preprocess(preprocessor, audios, output_dir)
            ]
        return batches

    @activity_defn(name=RUN_INFERENCE_ACTIVITY, progress_weight=_INFERENCE_WEIGHT)
    def infer(
        self,
        preprocessed_inputs: list[Path],
        config: InferenceRunnerConfig,
        *,
        progress: ProgressRateHandler | None = None,
    ) -> list[Path]:
        # TODO: fix this temporal by, we shouldn't have to reload
        config = _INFERENCE_CONFIG_TYPE_ADAPTER.validate_python(config)
        worker_config = ASRWorkerConfig()
        workdir = worker_config.workdir
        preprocessed_inputs = _LIST_OF_PATH_ADAPTER.validate_python(preprocessed_inputs)
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(preprocessed_inputs))
        batch_files = (workdir / batch_file for batch_file in preprocessed_inputs)
        # Audios paths in the input are relative to the batch file directory
        inputs = (
            [
                _relative_input(PreprocessedInput.model_validate(i), f.parent)
                for i in read_jsonl(f)
            ]
            for f in batch_files
        )
        audio_paths, inputs = tee(inputs)
        audio_paths = (
            i.metadata.preprocessed_file_path for b in audio_paths for i in b
        )
        # TODO: implement caching
        inference_runner = InferenceRunner.from_config(config)
        with inference_runner:
            # TODO: extract this into a function to improve testability
            paths = []
            for res_i, (path, asr_res) in enumerate(
                zip(audio_paths, inference_runner.process(inputs), strict=True)
            ):
                filename = f"{debuggable_name(path.name)}-transcript.json"
                transcript_path = workdir / safe_dir(filename) / filename
                transcript_path.parent.mkdir(parents=True, exist_ok=True)
                transcript_path.write_text(asr_res.model_dump_json())
                paths.append(transcript_path.relative_to(workdir))
                if progress is not None:
                    self._event_loop.run_until_complete(progress(res_i))
            return paths

    @activity_defn(name=POSTPROCESS_ACTIVITY, progress_weight=_BASE_WEIGHT)
    def postprocess(
        self,
        inference_results: list[Path],
        input_paths: list[Path],
        config: ParakeetPostprocessorConfig,
        project: str,
        *,
        progress: ProgressRateHandler | None = None,
    ) -> None:
        # TODO: this shouldn't be necessary, fix this bug
        input_paths = _LIST_OF_PATH_ADAPTER.validate_python(input_paths)
        config = ParakeetPostprocessorConfig.model_validate(config)
        worker_config = ASRWorkerConfig()
        workdir = worker_config.workdir
        artifacts_root = worker_config.artifacts_root
        inference_results = _LIST_OF_PATH_ADAPTER.validate_python(inference_results)
        inference_results = (
            ASRResult.model_validate_json((workdir / p).read_text())
            for p in inference_results
        )
        # TODO: implement caching
        postprocessor = Postprocessor.from_config(config)
        with postprocessor:
            if progress is not None:
                progress = to_raw_progress(progress, max_progress=len(input_paths))
            transcriptions = postprocessor.process(inference_results)
            # Strict is important here !
            for i, (original, asr_result) in enumerate(
                zip(input_paths, transcriptions, strict=True)
            ):
                t_path = write_transcription(
                    asr_result,
                    original.name,
                    artifacts_root=artifacts_root,
                    project=project,
                )
                activity.logger.debug("wrote transcription for %s", t_path)
                if progress is not None:
                    self._event_loop.run_until_complete(progress(i))


def preprocess(
    preprocessor: Preprocessor, audios: Iterable[Path], output_dir: Path
) -> Iterable[Path]:
    for batch_i, batch in enumerate(
        preprocessor.process(audios, output_dir=output_dir)
    ):
        # TODO: we might to create safe subdirs to avoid creating too many
        #  files in the same dir
        batch_file = output_dir / f"{batch_i}.jsonl"
        with batch_file.open("w") as f:
            for processed in batch:
                f.write(processed.model_dump_json() + "\n")
        yield batch_file


def write_transcription(
    asr_result: ASRResult,
    transcribed_filename: str,
    *,
    artifacts_root: Path,
    project: str,
) -> Path:
    result = Transcription.from_asr_handler_result(asr_result)
    artifact = result.model_dump_json().encode()
    # TODO: if transcriptions are too large we could also serialize them
    #  as jsonl
    rel_path = write_artifact(
        artifact,
        artifacts_root,
        project=project,
        filename=transcribed_filename,
        metadata_key=TRANSCRIPTION_METADATA_KEY,
        metadata_value=TRANSCRIPTION_METADATA_VALUE,
    )
    return rel_path


def _relative_input(
    preprocess_input: PreprocessedInput, root: Path
) -> PreprocessedInput:
    path = root / preprocess_input.metadata.preprocessed_file_path
    update = {"preprocessed_file_path": path}
    metadata = safe_copy(preprocess_input.metadata, update=update)
    return PreprocessedInput(metadata=metadata)  # noqa: F821


REGISTRY = [ASRActivities.preprocess, ASRActivities.infer, ASRActivities.postprocess]
