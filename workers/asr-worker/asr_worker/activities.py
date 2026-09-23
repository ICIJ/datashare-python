import json
import logging
from enum import StrEnum, unique
from functools import partial
from pathlib import Path
from typing import Annotated, Any, cast

from caul_core import (
    ASRResult,
    FSProcessedSegment,
    InferenceRunner,
    InferenceRunnerConfig,
    Postprocessor,
    Preprocessor,
    PreprocessorConfig,
)
from datashare_python.dependencies import lifespan_es_client, lifespan_worker_config
from datashare_python.objects import (
    PROCESSED_FILE_TA,
    WorkerRoots,
)
from datashare_python.types_ import (
    AsyncProgressRateHandler,
    SyncProgressRateHandler,
    Weight,
)
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    activity_workdir,
    config_cache_key,
    enter_cm,
    read_jsonl_as,
    to_raw_async_progress,
    to_raw_sync_progress,
)
from icij_common.pydantic_utils import safe_copy

from .aggregate import aggregate_results_act
from .config import ASRWorkerConfig
from .dependencies import (
    lifespan_inference_runner_cache,
    lifespan_postprocessor_cache,
    lifespan_preprocessor_cache,
)
from .es import index_transcriptions_act, search_audios_act
from .inference import infer_act
from .objects import ASRArgs, ASRIndexingConfig, ASRResponse
from .postprocessing import postprocess_act
from .preprocessing import preprocess_act

logger = logging.getLogger(__name__)

_BASE_WEIGHT = 1.0
_SEARCH_AUDIOS_WEIGHT = _BASE_WEIGHT * 2
_INDEX_AUDIOS_WEIGHT = _BASE_WEIGHT * 3
_PREPROCESS_WEIGHT = 5 * _BASE_WEIGHT
_INFERENCE_WEIGHT = 10 * _PREPROCESS_WEIGHT
_AGGREGATE_RESULT_WEIGHT = _SEARCH_AUDIOS_WEIGHT


@unique
class Activity(StrEnum):
    LOAD_WORKER_CONFIG = "asr.transcription.config"
    SEARCH_AUDIOS = "asr.transcription.search-audios"
    PREPROCESS = "asr.transcription.preprocess"
    INFER = "asr.transcription.infer"
    POSTPROCESS = "asr.transcription.postprocess"
    INDEX_TRANSCRIPTIONS = "asr.transcription.index"
    AGGREGATE_RESULTS = "asr.transcription.aggregate-results"


class ASRActivities(ActivityWithProgress):
    @activity_defn(name=Activity.SEARCH_AUDIOS)
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
        worker_config = lifespan_worker_config()
        workdir = worker_config.roots.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        batch_paths = [
            p.relative_to(workdir)
            async for p in search_audios_act(
                project,
                worker_config.roots,
                es_client,
                query,
                output_dir=output_dir,
                batch_size=batch_size,
            )
        ]
        return batch_paths

    @activity_defn(name=Activity.PREPROCESS)
    def preprocess(
        self,
        audio_batch: Path,
        project: str,
        config: PreprocessorConfig,
        *,
        progress: Annotated[  # noqa: ARG002
            SyncProgressRateHandler | None, Weight(value=_PREPROCESS_WEIGHT)
        ] = None,
    ) -> tuple[list[Path], Path, Path]:
        # Import caul.tasks to populate the Preprocessor registry
        import caul.tasks  # noqa: F401, PLC0415

        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        roots = worker_config.roots
        workdir = roots.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        audio_batch = workdir / audio_batch
        preprocessor_factory = enter_cm(partial(Preprocessor.from_config, config))
        preprocessor_key = config_cache_key(config)
        cache = lifespan_preprocessor_cache()
        preprocessor = cache.get_or_cache_resource(
            preprocessor_key, preprocessor_factory
        )
        audios = list(read_jsonl_as(audio_batch, PROCESSED_FILE_TA))
        batches, errors, audio_routes = preprocess_act(
            preprocessor, audios, worker_config, output_dir=output_dir
        )
        res_root = activity_workdir(workdir, project)
        res_root.mkdir(parents=True, exist_ok=True)
        batch_files = []
        for batch_i, batch in enumerate(batches):
            batch = (_relative_to_workdir(seg, res_root, roots) for seg in batch)  # noqa: PLW2901
            batch_file = output_dir / f"batch-{batch_i}.jsonl"
            batch_file.write_text("\n".join(seg.model_dump_json() for seg in batch))
            batch_files.append(batch_file.relative_to(workdir))
        errors_path = res_root / "errors.jsonl"
        errors_path.write_text("\n".join(p.model_dump_json() for p in errors))
        audio_routes_path = res_root / "routes.json"
        audio_routes_path.write_text(json.dumps(audio_routes))
        return batch_files, errors_path, audio_routes_path

    @activity_defn(name=Activity.INFER)
    async def infer(
        self,
        batches: list[Path],
        project: str,
        config: InferenceRunnerConfig,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None, Weight(value=_INFERENCE_WEIGHT)
        ] = None,
    ) -> tuple[list[Path], Path]:
        # Import caul.tasks to populate the InferenceRunner registry
        import caul.tasks  # noqa: F401, PLC0415

        n_batches = len(batches)
        if progress is not None:
            progress = to_raw_async_progress(progress, max_progress=n_batches)
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        workdir = worker_config.roots.workdir
        output_dir = activity_workdir(workdir, project)
        output_dir.mkdir(parents=True, exist_ok=True)
        batches = (workdir / p for p in batches)
        batches = (read_jsonl_as(b, FSProcessedSegment) for b in batches)
        batches = (
            tuple(safe_copy(seg, update={"path": workdir / seg.path}) for seg in b)
            for b in batches
        )
        device = worker_config.devices.inference
        logger.info("loading model %s on %s device", config.model, device)
        runner_factory = enter_cm(
            partial(InferenceRunner.from_config, config, device=device)
        )
        runner_key = config_cache_key(config)
        cache = lifespan_inference_runner_cache()
        inference_runner = cache.get_or_cache_resource(runner_key, runner_factory)
        logger.info(
            "model loaded, starting inference on %s audio chunks !",
            n_batches,
        )
        successes, errors = await infer_act(
            inference_runner, batches, output_dir=output_dir, progress=progress
        )
        inference_res = [p.relative_to(workdir) for p in successes]
        errors_path = output_dir / "errors.jsonl"
        errors_path.write_text("\n".join(e.model_dump_json() for e in errors))
        return inference_res, errors_path

    @activity_defn(name=Activity.POSTPROCESS)
    def postprocess(
        self,
        inference_results: list[Path],
        audio_routes: Path,
        args: ASRArgs,
        *,
        progress: Annotated[  # noqa: ARG002
            SyncProgressRateHandler | None, Weight(value=_BASE_WEIGHT)
        ] = None,
    ) -> tuple[Path, Path]:
        # Import caul.tasks to populate the Postprocessor‹ registry
        import caul.tasks  # noqa: F401, PLC0415

        worker_config = lifespan_worker_config()
        workdir = worker_config.roots.workdir
        artifacts_root = worker_config.roots.artifacts
        n_batches = len(inference_results)
        inference_results = (
            ASRResult.model_validate_json((workdir / p).read_text())
            for p in inference_results
        )
        audio_routes = json.loads(audio_routes.read_text())
        if progress is not None:
            progress = to_raw_sync_progress(progress, max_progress=n_batches)
        config = args.config.postprocessing
        postprocessor_factory = enter_cm(partial(Postprocessor.from_config, config))
        postprocessor_key = config_cache_key(config)
        cache = lifespan_postprocessor_cache()
        postprocessor = cache.get_or_cache_resource(
            postprocessor_key, postprocessor_factory
        )
        success, errors = postprocess_act(
            inference_results,
            audio_routes,
            postprocessor,
            args,
            artifacts_root=artifacts_root,
            event_loop=self._event_loop,
            progress=progress,
        )
        output_dir = activity_workdir(workdir, args.project)
        output_dir.mkdir(parents=True, exist_ok=True)
        successes_path = output_dir / "routes.jsonl"
        successes_path.write_text(json.dumps(success))
        errors_path = output_dir / "errors.jsonl"
        errors_path.write_text("\n".join(e.model_dump_json() for e in errors))
        return successes_path, errors_path

    @activity_defn(name=Activity.INDEX_TRANSCRIPTIONS)
    async def index_transcriptions(
        self,
        routes: Path,
        project: str,
        indexing_config: ASRIndexingConfig,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None, Weight(value=_INDEX_AUDIOS_WEIGHT)
        ] = None,
    ) -> int:
        worker_config = lifespan_worker_config()
        es_client = lifespan_es_client()
        target_bulk_char_size = worker_config.indexing.target_bulk_char_size
        routes = json.loads(routes.read_text())
        logger.info(
            "indexing %s transcriptions by bulk of about %s characters !",
            len(routes),
            target_bulk_char_size,
        )
        n_indexed = await index_transcriptions_act(
            routes,
            project,
            es_client,
            indexing_config=indexing_config,
            artifact_root=worker_config.roots.artifacts,
            target_bulk_char_size=target_bulk_char_size,
            progress=progress,
        )
        return n_indexed

    @activity_defn(name=Activity.AGGREGATE_RESULTS)
    async def aggregate_results(
        self,
        batches: list[Path],
        errors: list[Path],
        project: str,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None, Weight(value=_AGGREGATE_RESULT_WEIGHT)
        ] = None,
    ) -> ASRResponse:
        worker_config = cast(ASRWorkerConfig, lifespan_worker_config())
        roots = worker_config.roots
        logger.info("aggregating result!")
        report = await aggregate_results_act(batches, errors, project, roots)
        return report


def _relative_to_workdir(
    seg: FSProcessedSegment, output_dir: Path, roots: WorkerRoots
) -> FSProcessedSegment:
    abs_path = output_dir / seg.path
    rel_path = abs_path.relative_to(roots.workdir)
    return safe_copy(seg, update={"path": rel_path})


REGISTRY = [
    ASRActivities.search_audio_paths,
    ASRActivities.preprocess,
    ASRActivities.infer,
    ASRActivities.postprocess,
    ASRActivities.index_transcriptions,
    ASRActivities.aggregate_results,
]
