from collections.abc import AsyncGenerator, AsyncIterable, Callable, Generator, Iterable
from contextlib import AbstractContextManager, contextmanager
from itertools import tee
from pathlib import Path
from typing import Any

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
from icij_common.es import (
    DOC_CONTENT_TYPE,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    ESClient,
    and_query,
    has_type,
)
from icij_common.iter_utils import async_batches
from icij_common.pydantic_utils import safe_copy
from pydantic import TypeAdapter
from temporalio import activity

from asr_worker.utils import read_jsonl

from .config import ASRWorkerConfig
from .constants import (
    POSTPROCESS_ACTIVITY,
    PREPROCESS_ACTIVITY,
    RUN_INFERENCE_ACTIVITY,
    SEARCH_AUDIOS_ACTIVITY,
    SUPPORTED_CONTENT_TYPES,
    TRANSCRIPTION_METADATA_KEY,
    TRANSCRIPTION_METADATA_VALUE,
)
from .dependencies import lifespan_es_client
from .models import InferenceRunnerConfig, Transcription

_BASE_WEIGHT = 1.0
_SEARCH_AUDIOS_WEIGHT = _BASE_WEIGHT * 2
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])
_INFERENCE_CONFIG_TYPE_ADAPTER = TypeAdapter(InferenceRunnerConfig)


class ASRActivities(ActivityWithProgress):
    @activity_defn(name=SEARCH_AUDIOS_ACTIVITY, progress_weight=_SEARCH_AUDIOS_WEIGHT)
    async def search_audios(
        self, project: str, query: dict[str, Any], batch_size: int
    ) -> list[Path]:
        es_client = lifespan_es_client()
        worker_config = ASRWorkerConfig()
        batch_dir_name = activity_contextual_id()
        workdir = worker_config.workdir
        batch_root = workdir / batch_dir_name
        batch_root.mkdir(parents=True, exist_ok=True)
        # TODO: supported content types should be args
        query = search_audios(
            es_client, project, query, supported_content_types=SUPPORTED_CONTENT_TYPES
        )
        batch_paths = [
            p.relative_to(workdir)
            async for p in write_audio_search_results(query, batch_root, batch_size)
        ]
        return batch_paths

    @activity_defn(name=PREPROCESS_ACTIVITY, progress_weight=_PREPROCESS_WEIGHT)
    def preprocess(
        self, paths: list[Path] | Path, config: ParakeetPreprocessorConfig
    ) -> list[Path]:
        # TODO: this shouldn't be necessary, fix this bug
        worker_config = ASRWorkerConfig()
        audio_root = worker_config.audios_root
        workdir = worker_config.workdir
        # TODO: implement caching
        preprocessor = Preprocessor.from_config(config)
        contextual_id = activity_contextual_id()
        output_dir = workdir / contextual_id
        output_dir.mkdir(parents=True, exist_ok=True)
        if isinstance(paths, Path):
            audio_cm = _read_audio_ids(paths)
        else:
            paths = _LIST_OF_PATH_ADAPTER.validate_python(paths)
            audio_cm = _read_audios_cm(paths)
        with audio_cm() as audios, preprocessor:
            audios = (str(audio_root / p) for p in audios)  # noqa: PLW2901
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
    preprocessor: Preprocessor, audios: Iterable[str], output_dir: Path
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


async def write_audio_search_results(
    results: AsyncIterable[str], root: Path, batch_size: int
) -> AsyncIterable[Path]:
    batch_id = 0
    async for batch in async_batches(results, batch_size):
        batch_path = root / f"{batch_id}.txt"
        with batch_path.open("w") as f:
            for doc_id in batch:
                f.write(f"{doc_id}\n")
        yield batch_path
        batch_id += 1


_DOC_TYPE_QUERY = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)


async def search_audios(
    es_client: ESClient,
    project: str,
    query: dict[str, Any],
    supported_content_types: set[str],
) -> AsyncGenerator[str, None]:
    body = _with_audio_content(query, supported_content_types)
    async for page in es_client.poll_search_pages(
        index=project, body=body, sort="_doc:asc", _source=False
    ):
        for hit in page[HITS][HITS]:
            yield hit[ID_]


def _content_type_query(supported_content_types: set[str]) -> dict[str, Any]:
    content_type_query = {"terms": {DOC_CONTENT_TYPE: sorted(supported_content_types)}}
    doc_type = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
    return and_query(content_type_query, doc_type)


def _with_audio_content(
    query: dict[str, Any], supported_content_types: set[str]
) -> dict[str, Any]:
    type_query = _content_type_query(supported_content_types)
    if not query:
        return type_query
    return and_query(query, type_query[QUERY])


def _read_audio_ids(path: Path) -> AbstractContextManager:
    @contextmanager
    def cm() -> Generator[Iterable[Path], None, None]:
        with open(path) as f:
            yield (Path(line.strip()) for line in f)

    return cm


def _read_audios_cm(paths: list[Path]) -> Callable[[], AbstractContextManager]:
    @contextmanager
    def cm() -> Generator[Iterable[Path], None, None]:
        yield iter(paths)

    return cm


REGISTRY = [
    ASRActivities.search_audios,
    ASRActivities.preprocess,
    ASRActivities.infer,
    ASRActivities.postprocess,
]
