import json
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable
from io import TextIOWrapper
from itertools import tee
from pathlib import Path
from typing import Any, TextIO, cast

from caul.objects import ASRResult, PreprocessedInput
from caul.tasks import (
    InferenceRunner,
    ParakeetPostprocessorConfig,
    ParakeetPreprocessorConfig,
    Postprocessor,
    Preprocessor,
)
from datashare_python.dependencies import lifespan_worker_config
from datashare_python.types_ import ProgressRateHandler, RawProgressHandler
from datashare_python.utils import (
    ActivityWithProgress,
    DocArtifact,
    activity_contextual_id,
    activity_defn,
    debuggable_name,
    safe_dir,
    to_raw_progress,
    write_artifact,
)
from icij_common.es import (
    DOC_CONTENT_TYPE,
    DOC_PATH,
    ES_DOCUMENT_TYPE,
    HITS,
    ID_,
    QUERY,
    SOURCE,
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
from .models import DocId, InferenceRunnerConfig, Transcription

_BASE_WEIGHT = 1.0
_SEARCH_AUDIOS_WEIGHT = _BASE_WEIGHT * 2
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])
_INFERENCE_CONFIG_TYPE_ADAPTER = TypeAdapter(InferenceRunnerConfig)


class ASRActivities(ActivityWithProgress):
    @activity_defn(name=SEARCH_AUDIOS_ACTIVITY, progress_weight=_SEARCH_AUDIOS_WEIGHT)
    async def search_audio_paths(
        self, project: str, query: dict[str, Any], batch_size: int
    ) -> list[Path]:
        es_client = lifespan_es_client()
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        batch_dir_name = activity_contextual_id()
        workdir = worker_config.workdir
        output_dir = workdir / batch_dir_name
        output_dir.mkdir(parents=True, exist_ok=True)
        batch_paths = [
            p.relative_to(workdir)
            async for p in search_audio_paths_act(
                project, es_client, query, output_dir, batch_size
            )
        ]
        return batch_paths

    @activity_defn(name=PREPROCESS_ACTIVITY, progress_weight=_PREPROCESS_WEIGHT)
    def preprocess(
        self, audio_batch: Path, config: ParakeetPreprocessorConfig
    ) -> list[Path]:
        # TODO: this shouldn't be necessary, fix this bug
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        audio_root = worker_config.audios_root
        workdir = worker_config.workdir
        # TODO: implement caching
        preprocessor = Preprocessor.from_config(config)
        contextual_id = activity_contextual_id()
        output_dir = workdir / contextual_id
        output_dir.mkdir(parents=True, exist_ok=True)
        audio_batch = workdir / audio_batch
        with preprocessor:
            batch_paths = preprocess_act(
                preprocessor,
                audio_batch,
                audio_root=audio_root,
                output_dir=output_dir,
            )
            batches = [p.relative_to(workdir) for p in batch_paths]
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
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        preprocessed_inputs = _LIST_OF_PATH_ADAPTER.validate_python(preprocessed_inputs)
        preprocessed_inputs = [workdir / p for p in preprocessed_inputs]
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(preprocessed_inputs))
        inference_runner = InferenceRunner.from_config(config)
        with inference_runner:
            paths = infer_act(
                inference_runner,
                preprocessed_inputs,
                workdir,
                event_loop=self._event_loop,
                progress=progress,
            )
        return [p.relative_to(workdir) for p in paths]

    @activity_defn(name=POSTPROCESS_ACTIVITY, progress_weight=_BASE_WEIGHT)
    def postprocess(
        self,
        inference_results: list[Path],
        audio_batch: Path,
        config: ParakeetPostprocessorConfig,
        project: str,
        *,
        progress: ProgressRateHandler | None = None,
    ) -> int:
        # TODO: this shouldn't be necessary, fix this bug
        config = ParakeetPostprocessorConfig.model_validate(config)
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        audio_batch = workdir / audio_batch
        artifacts_root = worker_config.artifacts_root
        inference_results = _LIST_OF_PATH_ADAPTER.validate_python(inference_results)
        inference_results = (
            ASRResult.model_validate_json((workdir / p).read_text())
            for p in inference_results
        )
        # TODO: implement caching
        postprocessor = Postprocessor.from_config(config)
        with postprocessor:
            with audio_batch.open() as f:
                doc_ids = [doc_id for doc_id, _ in read_batch(f)]
            if progress is not None:
                progress = to_raw_progress(progress, max_progress=len(doc_ids))
            return postprocess_act(
                postprocessor,
                inference_results,
                doc_ids,
                project=project,
                artifacts_root=artifacts_root,
                event_loop=self._event_loop,
                progress=progress,
            )


async def search_audio_paths_act(
    project: str,
    es_client: ESClient,
    query: dict[str, Any],
    output_dir: Path,
    batch_size: int,
) -> AsyncIterable[Path]:
    # TODO: supported content types should be args
    query = _search_audio_paths(
        es_client, project, query, supported_content_types=SUPPORTED_CONTENT_TYPES
    )
    async for p in write_audio_batches(query, output_dir, batch_size):
        yield p


def preprocess_act(
    preprocessor: Preprocessor,
    audio_batch: Path,
    *,
    audio_root: Path,
    output_dir: Path,
) -> list[Path]:
    with audio_batch.open() as f:
        audios = read_batch(f)
        audios = (str(audio_root / p) for _, p in audios)
        # TODO: implement a caching strategy here, we could avoid processing files
        #  which have already been preprocessed
        return list(_preprocess(preprocessor, audios, output_dir))


def infer_act(
    inference_runner: InferenceRunner,
    preprocessed_inputs: list[Path],
    output_dir: Path,
    event_loop: AbstractEventLoop | None = None,
    progress: RawProgressHandler | None = None,
) -> list[Path]:
    # Audios paths in the input are relative to the batch file directory
    inputs = (
        [
            _relative_input(PreprocessedInput.model_validate(i), f.parent)
            for i in read_jsonl(f)
        ]
        for f in preprocessed_inputs
    )
    audio_paths, inputs = tee(inputs)
    audio_paths = (i.metadata.preprocessed_file_path for b in audio_paths for i in b)
    # TODO: implement caching
    paths = []
    for res_i, (path, asr_res) in enumerate(
        zip(audio_paths, inference_runner.process(inputs), strict=True)
    ):
        filename = f"{debuggable_name(path.name)}-transcript.json"
        transcript_path = output_dir / safe_dir(filename) / filename
        transcript_path.parent.mkdir(parents=True, exist_ok=True)
        transcript_path.write_text(asr_res.model_dump_json())
        paths.append(transcript_path)
        if progress is not None and event_loop is not None:
            event_loop.run_until_complete(progress(res_i))
    return paths


def postprocess_act(
    postprocessor: Postprocessor,
    inference_results: Iterable[ASRResult],
    doc_ids: Iterable[str],
    *,
    artifacts_root: Path,
    project: str,
    event_loop: AbstractEventLoop | None = None,
    progress: ProgressRateHandler | None = None,
) -> int:
    transcriptions = postprocessor.process(inference_results)
    # Strict is important here !
    n_docs = 0
    for i, (doc_id, asr_result) in enumerate(zip(doc_ids, transcriptions, strict=True)):
        n_docs += 1
        t_path = write_transcription(
            doc_id, asr_result, artifacts_root=artifacts_root, project=project
        )
        activity.logger.debug("wrote transcription for %s", t_path)
        if progress is not None and event_loop is not None:
            event_loop.run_until_complete(progress(i))
    return n_docs


def _preprocess(
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
    doc_id: str, asr_result: ASRResult, *, artifacts_root: Path, project: str
) -> Path:
    result = Transcription.from_asr_handler_result(asr_result)
    artifact_bytes = result.model_dump_json().encode()
    artifact = DocArtifact(
        project=project,
        doc_id=doc_id,
        filename=TRANSCRIPTION_METADATA_VALUE,
        metadata_key=TRANSCRIPTION_METADATA_KEY,
        artifact=artifact_bytes,
    )
    # TODO: if transcriptions are too large we could also serialize them
    #  as jsonl
    rel_path = write_artifact(artifacts_root, artifact)
    return rel_path


def _relative_input(
    preprocess_input: PreprocessedInput, root: Path
) -> PreprocessedInput:
    path = root / preprocess_input.metadata.preprocessed_file_path
    update = {"preprocessed_file_path": path}
    metadata = safe_copy(preprocess_input.metadata, update=update)
    return PreprocessedInput(metadata=metadata)  # noqa: F821


async def write_audio_batches(
    ids_and_paths: AsyncIterable[tuple[DocId, str]], root: Path, batch_size: int
) -> AsyncIterable[Path]:
    batch_id = 0
    async for batch in async_batches(ids_and_paths, batch_size):
        batch_path = root / f"{batch_id}.txt"
        with batch_path.open("w") as f:
            write_audio_batch(batch, f)
        yield batch_path
        batch_id += 1


def write_audio_batch(batch: Iterable[tuple[DocId, Path]], f: TextIOWrapper) -> None:
    for doc_id, path in batch:
        data = {"doc_id": doc_id, "path": str(path)}
        f.write(f"{json.dumps(data)}\n")


_DOC_TYPE_QUERY = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
_DOC_CONTENT_SOURCES = [DOC_PATH]


async def _search_audio_paths(
    es_client: ESClient,
    project: str,
    query: dict[str, Any],
    supported_content_types: set[str],
) -> AsyncGenerator[tuple[DocId, str], None]:
    body = _with_audio_content(query, supported_content_types)
    async for page in es_client.poll_search_pages(
        index=project, body=body, sort="_doc:asc", _source_includes=_DOC_CONTENT_SOURCES
    ):
        for hit in page[HITS][HITS]:
            yield hit[ID_], hit[SOURCE][DOC_PATH]


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


def read_batch(f: TextIO) -> Generator[tuple[DocId, Path], None, None]:
    for line in f:
        data = json.loads(line)
        yield data["doc_id"], Path(data["path"])


REGISTRY = [
    ASRActivities.search_audio_paths,
    ASRActivities.preprocess,
    ASRActivities.infer,
    ASRActivities.postprocess,
]
