import asyncio
import logging
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from functools import partial
from itertools import tee
from pathlib import Path
from typing import Annotated, Any, Protocol, cast

from caul_core import (
    ASRResult,
    InferenceRunner,
    InferenceRunnerConfig,
    Postprocessor,
    PostprocessorConfig,
    PreprocessedInput,
    Preprocessor,
    PreprocessorConfig,
)
from datashare_python.dependencies import lifespan_worker_config
from datashare_python.objects import (
    Document,
    FilesystemDocument,
)
from datashare_python.types_ import (
    AsyncProgressRateHandler,
    RawAsyncProgressHandler,
    SyncProgressRateHandler,
    Weight,
)
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    activity_workdir,
    config_cache_key,
    debuggable_name,
    enter_cm,
    read_jsonl,
    safe_dir,
    symlink_embedded_document_to_workdir,
    to_raw_async_progress,
    to_raw_sync_progress,
    write_artifact,
)
from icij_common.es import (
    DOC_CONTENT_TYPE,
    DOC_LANGUAGE,
    DOC_METADATA,
    DOC_PATH,
    DOC_ROOT_ID,
    ES_DOCUMENT_TYPE,
    HITS,
    QUERY,
    ESClient,
    and_query,
    has_type,
)
from icij_common.iter_utils import async_batches
from icij_common.pydantic_utils import safe_copy
from pydantic import TypeAdapter

from .config import ASRWorkerConfig
from .constants import (
    POSTPROCESS_ACTIVITY,
    PREPROCESS_ACTIVITY,
    RUN_INFERENCE_ACTIVITY,
    SEARCH_AUDIOS_ACTIVITY,
    SUPPORTED_CONTENT_TYPES,
)
from .dependencies import (
    lifespan_es_client,
    lifespan_inference_runner_cache,
    lifespan_postprocessor_cache,
    lifespan_preprocessor_cache,
)
from .objects import (
    ASRArgs,
    Transcription,
    TranscriptionArtifact,
    TranscriptionManifestEntry,
)

logger = logging.getLogger(__name__)

_BASE_WEIGHT = 1.0
_SEARCH_AUDIOS_WEIGHT = _BASE_WEIGHT * 2
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT

_LIST_OF_PATH_ADAPTER = TypeAdapter(list[Path])


class ArtifactFactory(Protocol):
    def __call__(self, artifact: bytes) -> TranscriptionArtifact: ...


class ASRActivities(ActivityWithProgress):
    @activity_defn(name=SEARCH_AUDIOS_ACTIVITY)
    async def search_audio_paths(
        self,
        project: str,
        query: dict[str, Any],
        batch_size: int,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None, Weight(value=_SEARCH_AUDIOS_WEIGHT)
        ] = None,
    ) -> list[Path]:
        es_client = lifespan_es_client()
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        batch_paths = [
            p.relative_to(workdir)
            async for p in search_audio_paths_act(
                project,
                es_client,
                query,
                config=worker_config,
                output_dir=output_dir,
                batch_size=batch_size,
            )
        ]
        return batch_paths

    @activity_defn(name=PREPROCESS_ACTIVITY)
    def preprocess(
        self,
        audio_batch: Path,
        project: str,
        config: PreprocessorConfig,
        *,
        progress: Annotated[  # noqa: ARG002
            SyncProgressRateHandler | None, Weight(value=_PREPROCESS_WEIGHT)
        ] = None,
    ) -> list[Path]:
        # Import caul.tasks to populate the Preprocessor registry
        import caul.tasks  # noqa: F401, PLC0415

        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        audio_batch = workdir / audio_batch
        preprocessor_factory = enter_cm(partial(Preprocessor.from_config, config))
        preprocessor_key = config_cache_key(config)
        cache = lifespan_preprocessor_cache()
        preprocessor = cache.get_or_cache_resource(
            preprocessor_key, preprocessor_factory
        )
        batch_paths = preprocess_act(
            preprocessor,
            audio_batch,
            worker_config=worker_config,
            output_dir=output_dir,
        )
        batches = [p.relative_to(workdir) for p in batch_paths]
        return batches

    @activity_defn(name=RUN_INFERENCE_ACTIVITY)
    async def infer(
        self,
        preprocessed_inputs: list[Path],
        project: str,
        config: InferenceRunnerConfig,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None, Weight(value=_INFERENCE_WEIGHT)
        ] = None,
    ) -> list[Path]:
        # Import caul.tasks to populate the InferenceRunner registry
        import caul.tasks  # noqa: F401, PLC0415

        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        preprocessed_inputs = _LIST_OF_PATH_ADAPTER.validate_python(preprocessed_inputs)
        preprocessed_inputs = [workdir / p for p in preprocessed_inputs]
        if progress is not None:
            progress = to_raw_async_progress(
                progress, max_progress=len(preprocessed_inputs)
            )
        logger.info("loading model %s", config.model)
        runner_factory = enter_cm(partial(InferenceRunner.from_config, config))
        runner_key = config_cache_key(config)
        cache = lifespan_inference_runner_cache()
        inference_runner = cache.get_or_cache_resource(runner_key, runner_factory)
        logger.info(
            "model loaded, starting inference on %s audio chunks !",
            len(preprocessed_inputs),
        )
        inference_res = infer_act(
            inference_runner,
            preprocessed_inputs,
            output_dir=output_dir,
            progress=progress,
        )
        inference_res = [p.relative_to(workdir) async for p in inference_res]
        return inference_res

    @activity_defn(name=POSTPROCESS_ACTIVITY)
    def postprocess(
        self,
        inference_results: list[Path],
        audio_batch: Path,
        config: PostprocessorConfig,
        args: ASRArgs,
        *,
        progress: Annotated[  # noqa: ARG002
            SyncProgressRateHandler | None, Weight(value=_BASE_WEIGHT)
        ] = None,
    ) -> int:
        # Import caul.tasks to populate the Postprocessor‹ registry
        import caul.tasks  # noqa: F401, PLC0415

        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.workdir
        audio_batch = workdir / audio_batch
        artifacts_root = worker_config.artifacts_root
        inference_results = _LIST_OF_PATH_ADAPTER.validate_python(inference_results)
        inference_results = (
            ASRResult.model_validate_json((workdir / p).read_text())
            for p in inference_results
        )

        docs = (
            FilesystemDocument.model_validate(fs_doc)
            for fs_doc in read_jsonl(audio_batch)
        )
        doc_ids = [doc.id for doc in docs]
        if progress is not None:
            progress = to_raw_sync_progress(progress, max_progress=len(doc_ids))
        postprocessor_factory = enter_cm(partial(Postprocessor.from_config, config))
        postprocessor_key = config_cache_key(config)
        cache = lifespan_postprocessor_cache()
        postprocessor = cache.get_or_cache_resource(
            postprocessor_key, postprocessor_factory
        )
        return postprocess_act(
            postprocessor,
            inference_results,
            doc_ids,
            args,
            artifacts_root=artifacts_root,
            event_loop=self._event_loop,
            progress=progress,
        )


async def search_audio_paths_act(
    project: str,
    es_client: ESClient,
    query: dict[str, Any],
    *,
    config: ASRWorkerConfig,
    output_dir: Path,
    batch_size: int,
) -> AsyncIterable[Path]:
    # TODO: supported content types should be args
    docs = _search_audio_paths(
        es_client, project, query, supported_content_types=SUPPORTED_CONTENT_TYPES
    )
    docs = (
        symlink_embedded_document_to_workdir(
            d, config.artifacts_root, workdir=config.workdir
        )
        async for d in docs
    )
    async for p in write_audio_batches(docs, output_dir, batch_size):
        yield p


def preprocess_act(
    preprocessor: Preprocessor,
    audio_batch: Path,
    *,
    worker_config: ASRWorkerConfig,
    output_dir: Path,
) -> list[Path]:
    docs_root = worker_config.docs_root
    artifacts_root = worker_config.artifacts_root
    workdir = worker_config.workdir
    logger.debug("locating files...")
    audios = (
        FilesystemDocument.model_validate(fs_doc) for fs_doc in read_jsonl(audio_batch)
    )
    audios = (
        fs_doc.locate(
            original_root=docs_root, artifacts_root=artifacts_root, workdir=workdir
        )
        for fs_doc in audios
    )
    audios = (str(a) for a in audios)
    # TODO: implement a caching strategy here, we could avoid processing files
    #  which have already been preprocessed
    logger.debug("starting preprocessing...")
    return list(_preprocess(preprocessor, audios, output_dir))


async def infer_act(
    inference_runner: InferenceRunner,
    preprocessed_inputs: list[Path],
    output_dir: Path,
    event_loop: AbstractEventLoop | None = None,
    progress: RawAsyncProgressHandler | None = None,
) -> AsyncIterable[Path]:
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
    inference_results = await asyncio.to_thread(
        _transcribe_as_list, inference_runner, list(inputs)
    )
    for res_i, (path, asr_res) in enumerate(
        zip(audio_paths, inference_results, strict=True)
    ):
        filename = f"{debuggable_name(path.name)}-transcript.json"
        transcript_path = output_dir / safe_dir(filename) / filename
        transcript_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(
            "run inference for %s, writing result to %s", path, transcript_path
        )
        transcript_path.write_text(asr_res.model_dump_json())
        yield transcript_path
        if progress is not None and event_loop is not None:
            await progress(res_i)


def _transcribe_as_list(
    inference_runner: InferenceRunner, inputs: Iterable[list[PreprocessedInput]]
) -> list[ASRResult]:
    return list(inference_runner.process(inputs))


def postprocess_act(
    postprocessor: Postprocessor,
    inference_results: Iterable[ASRResult],
    doc_ids: Iterable[str],
    args: ASRArgs,
    *,
    artifacts_root: Path,
    event_loop: AbstractEventLoop | None = None,
    progress: SyncProgressRateHandler | None = None,
) -> int:
    transcriptions = postprocessor.process(inference_results)
    # Strict is important here !
    n_docs = 0
    for i, (doc_id, asr_result) in enumerate(zip(doc_ids, transcriptions, strict=True)):
        n_docs += 1
        manifest_entry = TranscriptionManifestEntry.complete(
            args, confidence=asr_result.score
        )
        artifact_factory = partial(
            TranscriptionArtifact,
            project=args.project,
            doc_id=doc_id,
            manifest_entry=manifest_entry,
        )
        t_path = write_transcription(asr_result, artifact_factory, artifacts_root)
        logger.debug("wrote transcription for %s", t_path)
        if progress is not None and event_loop is not None:
            progress(i, event_loop)
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
        logger.debug("writing batch to %s", batch_file)
        with batch_file.open("w") as f:
            for processed in batch:
                f.write(processed.model_dump_json() + "\n")
        yield batch_file


def write_transcription(
    asr_result: ASRResult, artifact_factory: ArtifactFactory, artifacts_root: Path
) -> Path:
    result = Transcription.from_asr_handler_result(asr_result)
    artifact_bytes = result.model_dump_json().encode()
    artifact = artifact_factory(artifact=artifact_bytes)
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
    docs: AsyncIterable[FilesystemDocument], root: Path, batch_size: int
) -> AsyncIterable[Path]:
    batch_id = 0
    async for batch in async_batches(docs, batch_size):
        batch_path = root / f"{batch_id}.txt"
        with batch_path.open("w") as f:
            for fs_doc in batch:
                f.write(f"{fs_doc.model_dump_json()}\n")
        yield batch_path
        batch_id += 1


_DOC_TYPE_QUERY = has_type(type_field="type", type_value=ES_DOCUMENT_TYPE)
_DOC_CONTENT_SOURCES = [DOC_PATH, DOC_ROOT_ID, DOC_LANGUAGE, DOC_METADATA]


async def _search_audio_paths(
    es_client: ESClient,
    project: str,
    query: dict[str, Any],
    supported_content_types: set[str],
) -> AsyncGenerator[FilesystemDocument, None]:
    body = _with_audio_content(query, supported_content_types)
    async for page in es_client.poll_search_pages(
        index=project, body=body, sort="_doc:asc", _source_includes=_DOC_CONTENT_SOURCES
    ):
        for hit in page[HITS][HITS]:
            yield Document.from_es(hit).to_filesystem()


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


REGISTRY = [
    ASRActivities.search_audio_paths,
    ASRActivities.preprocess,
    ASRActivities.infer,
    ASRActivities.postprocess,
]
