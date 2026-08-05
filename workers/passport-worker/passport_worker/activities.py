import logging
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Annotated, cast

from aiofile import async_open
from datashare_python.dependencies import lifespan_es_client, lifespan_worker_config
from datashare_python.types_ import (
    AsyncProgressRateHandler,
    SyncProgressRateHandler,
    Weight,
)
from datashare_python.utils import (
    ActivityWithProgress,
    activity_defn,
    activity_workdir,
    async_enter_cm,
    config_cache_key,
    enter_cm,
)

from .aggregate import aggregate_results_act
from .config import PassportWorkerConfig
from .dependencies import (
    lifespan_image_preprocessor_cache,
    lifespan_passport_detector_cache,
    lifespan_pdf_converter_cache,
)
from .inference import (
    PassportDetector,
    create_inference_batches_act,
    detect_passports_act,
)
from .objects import (
    DocId,
    DocumentSearchQuery,
    ImagePreprocessorConfig,
    PassportDetectionArgs,
    PassportDetectionResponse,
    PDFConverterConfig,
    PreprocessingBatches,
)
from .preprocessing import (
    ImagePreprocessor,
    PDFConverter,
    convert_to_pdfs_act,
    preprocess_images_act,
    preprocess_pdfs_act,
)
from .search import create_preprocessing_batches_act

logger = logging.getLogger(__name__)

_BASE_WEIGHT = 1.0
_CREATE_PREPROCESSING_BATCHES_WEIGHT = _BASE_WEIGHT * 1
_PREPROCESS_IMAGES_WEIGHT = _BASE_WEIGHT * 5
_CONVERT_TO_PDF_WEIGHT = _BASE_WEIGHT * 7
_PREPROCESS_PDF_WEIGHT = _BASE_WEIGHT * 3
_CREATE_INFERENCE_BATCH_WEIGHT = _CREATE_PREPROCESSING_BATCHES_WEIGHT * 1


class Activity(StrEnum):
    CREATE_PREPROCESSING_BATCHES = "passport-detection.create-preprocessing-batches"
    PREPROCESS_IMAGES = "passport-detection.preprocess.images"
    CONVERT_TO_PDFS = "passport-detection.convert-to-pdf"
    PREPROCESS_PDFS = "passport-detection.preprocess.pdfs"
    CREATE_INFERENCE_BATCHES = "passport-detection.create-inference-batches"
    DETECT_PASSPORTS = "passport-detection.detect-passports"
    AGGREGATE_RESULTS = "passport-detection.aggregate-results"


class PassportDetectionActivities(ActivityWithProgress):
    @activity_defn(name=Activity.CREATE_PREPROCESSING_BATCHES)
    async def create_preprocessing_batches(
        self,
        docs: list[DocId] | DocumentSearchQuery | None,
        project: str,
        *,
        progress: Annotated[  # noqa: ARG002
            AsyncProgressRateHandler | None,
            Weight(value=_CREATE_PREPROCESSING_BATCHES_WEIGHT),
        ] = None,
    ) -> PreprocessingBatches:
        es_client = lifespan_es_client()
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        workdir = worker_config.paths.workdir
        output_root = activity_workdir(workdir, project, act_context=True)
        output_root.mkdir(parents=True, exist_ok=True)
        target_n_pages_per_batch = worker_config.preprocessing.target_n_pages_per_batch
        return await create_preprocessing_batches_act(
            docs,
            project,
            es_client,
            worker_config.paths,
            target_n_pages_per_batch,
            output_root=output_root,
        )

    @activity_defn(name=Activity.PREPROCESS_IMAGES)
    def preprocess_images(
        self,
        batch: Path,
        project: str,
        config: ImagePreprocessorConfig,
        *,
        progress: Annotated[
            SyncProgressRateHandler | None, Weight(value=_PREPROCESS_IMAGES_WEIGHT)
        ] = None,
    ) -> tuple[Path, Path]:
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        workdir = worker_config.paths.workdir
        logger.info("loading image preprocessor...")
        cache = lifespan_image_preprocessor_cache()
        image_preprocessor_cache_key = config_cache_key(config)
        image_preprocessor_factory = enter_cm(
            partial(ImagePreprocessor.from_config, config)
        )
        image_preprocessor = cache.get_or_cache_resource(
            image_preprocessor_cache_key, image_preprocessor_factory
        )
        logger.info("loaded image preprocessor !")
        pages_root = activity_workdir(workdir, project, act_context=False)
        pages_root.mkdir(parents=True, exist_ok=True)
        executor = worker_config.to_image_preprocessing_executor()
        chunk_size = worker_config.preprocessing.images.chunk_size
        success, errors = preprocess_images_act(
            batch,
            worker_config.paths,
            output_root=pages_root,
            image_preprocessor=image_preprocessor,
            executor=executor,
            chunk_size=chunk_size,
            event_loop=self._event_loop,
            progress=progress,
        )
        res_root = activity_workdir(workdir, project, act_context=True)
        res_root.mkdir(parents=True, exist_ok=True)
        successes_path = res_root / "pages.jsonl"
        successes_path.write_text("\n".join(p.model_dump_json() for p in success))
        errors_path = res_root / "errors.jsonl"
        errors_path.write_text("\n".join(p.model_dump_json() for p in errors))
        return successes_path, errors_path

    @activity_defn(name=Activity.CONVERT_TO_PDFS)
    async def convert_to_pdfs(
        self,
        batch: Path,
        project: str,
        config: PDFConverterConfig,
        *,
        progress: Annotated[
            AsyncProgressRateHandler | None, Weight(value=_CONVERT_TO_PDF_WEIGHT)
        ] = None,
    ) -> tuple[Path, Path]:
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        cache = lifespan_pdf_converter_cache()
        pdf_converter_cache_key = config_cache_key(config)
        pdf_converter_factory = async_enter_cm(
            partial(PDFConverter.from_config, config)
        )
        pdf_converter = await cache.async_get_or_cache_resource(
            pdf_converter_cache_key, pdf_converter_factory
        )
        workdir = worker_config.paths.workdir
        pdfs_root = activity_workdir(workdir, project, act_context=False)
        pdfs_root.mkdir(parents=True, exist_ok=True)
        max_concurrency = worker_config.preprocessing.pdfs.max_concurrency
        successes, errors = await convert_to_pdfs_act(
            batch,
            pdf_converter,
            worker_config.paths,
            max_concurrency,
            output_root=pdfs_root,
            progress=progress,
        )
        res_root = activity_workdir(workdir, project, act_context=True)
        res_root.mkdir(parents=True, exist_ok=True)
        pdf_paths = res_root / "pdfs.jsonl"
        async with async_open(pdf_paths, "w") as f:
            await f.write("\n".join(d.model_dump_json() for d in successes))
        errors_path = res_root / "errors.jsonl"
        async with async_open(errors_path, "w") as f:
            await f.write("\n".join(e.model_dump_json() for e in errors))
        return pdf_paths, errors_path

    @activity_defn(name=Activity.PREPROCESS_PDFS)
    async def preprocess_pdfs(
        self,
        batch: Path,
        project: str,
        *,
        progress: Annotated[
            AsyncProgressRateHandler | None, Weight(value=_PREPROCESS_PDF_WEIGHT)
        ] = None,
    ) -> tuple[Path, Path]:
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        workdir = worker_config.paths.workdir
        output_root = activity_workdir(workdir, project, act_context=False)
        output_root.mkdir(parents=True, exist_ok=True)
        successes, errors = await preprocess_pdfs_act(
            batch, worker_config.paths, output_root=output_root, progress=progress
        )
        res_root = activity_workdir(workdir, project, act_context=True)
        res_root.mkdir(parents=True, exist_ok=True)
        pdf_paths = res_root / "pdfs.jsonl"
        async with async_open(pdf_paths, "w") as f:
            await f.write("\n".join(d.model_dump_json() for d in successes))
        errors_path = res_root / "errors.jsonl"
        async with async_open(errors_path, "w") as f:
            await f.write("\n".join(e.model_dump_json() for e in errors))
        return pdf_paths, errors_path

    @activity_defn(name=Activity.CREATE_INFERENCE_BATCHES)
    async def create_inference_batches(
        self,
        batches: list[Path],
        project: str,
        *,
        progress: Annotated[  # noqa:ARG002
            AsyncProgressRateHandler | None,
            Weight(value=_CREATE_INFERENCE_BATCH_WEIGHT),
        ] = None,
    ) -> list[Path]:
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        batch_size = worker_config.inference.batch_size
        batches_per_task = worker_config.inference.batches_per_task
        worker_paths = worker_config.paths
        output_root = activity_workdir(worker_paths.workdir, project)
        output_root.mkdir(parents=True, exist_ok=True)
        return await create_inference_batches_act(
            batches,
            worker_paths,
            output_root,
            target_batches_per_task=batches_per_task,
            inference_batch_size=batch_size,
        )

    @activity_defn(name=Activity.DETECT_PASSPORTS)
    async def detect_passports(
        self,
        batch: Path,
        args: PassportDetectionArgs,
        *,
        progress: Annotated[
            AsyncProgressRateHandler | None,
            Weight(value=_CREATE_INFERENCE_BATCH_WEIGHT),
        ] = None,
    ) -> Path:
        logger.info("loading passport detector...")
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        batch_size = worker_config.inference.batch_size
        cache = lifespan_passport_detector_cache()
        passport_detector_config = args.config.inference.passport_detector
        passport_detector_key = config_cache_key(passport_detector_config)
        passport_detector_factory = enter_cm(
            partial(PassportDetector.from_config, passport_detector_config)
        )
        passport_detector = cache.get_or_cache_resource(
            passport_detector_key, passport_detector_factory
        )
        logger.info("passport detector loaded !")
        workdir = worker_config.paths.workdir
        res_root = activity_workdir(workdir, args.project, act_context=True)
        res_root.mkdir(parents=True, exist_ok=True)
        res = await detect_passports_act(
            batch,
            passport_detector,
            worker_config.paths,
            args,
            batch_size=batch_size,
            progress=progress,
        )
        result_path = res_root / "inference_results.json"
        async with async_open(result_path, "w") as f:
            await f.write(res.model_dump_json())
        return result_path

    @activity_defn(name=Activity.AGGREGATE_RESULTS)
    async def aggregate_results(
        self,
        error_paths: list[Path],
        result_paths: list[Path],
        *,
        progress: Annotated[  # noqa:ARG002
            AsyncProgressRateHandler | None,
            Weight(value=_CREATE_INFERENCE_BATCH_WEIGHT),
        ] = None,
    ) -> PassportDetectionResponse:
        worker_config = cast(PassportWorkerConfig, lifespan_worker_config())
        res = await aggregate_results_act(
            error_paths, result_paths=result_paths, paths=worker_config.paths
        )
        return res


ACTIVITIES = [
    PassportDetectionActivities.create_preprocessing_batches,
    PassportDetectionActivities.preprocess_images,
    PassportDetectionActivities.convert_to_pdfs,
    PassportDetectionActivities.preprocess_pdfs,
    PassportDetectionActivities.create_inference_batches,
    PassportDetectionActivities.detect_passports,
    PassportDetectionActivities.aggregate_results,
]
