import logging
import multiprocessing
import platform
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    lifespan_worker_config,
    set_es_client,
    set_worker_config,
)
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.utils import SharedResources

from asr_worker.config import ASRWorkerConfig

logger = logging.getLogger(__name__)

_PREPROCESSORS: ContextVar[SharedResources] = ContextVar("preprocessor_cache")
_INFERENCE_RUNNERS: ContextVar[SharedResources] = ContextVar("inference_runner_cache")
_POSTPROCESSORS: ContextVar[SharedResources] = ContextVar("postprocessor_cache")


def set_preprocessor_cache(worker_config: ASRWorkerConfig) -> SharedResources:
    cache = worker_config.cache.preprocessor.to_resource_cache()
    _PREPROCESSORS.set(cache)
    return cache


def lifespan_preprocessor_cache() -> SharedResources:
    try:
        return _PREPROCESSORS.get()
    except LookupError as e:
        raise DependencyInjectionError("preprocessor cache") from e


def set_inference_runner_cache(worker_config: ASRWorkerConfig) -> SharedResources:
    cache = worker_config.cache.inference_runner.to_resource_cache()
    _INFERENCE_RUNNERS.set(cache)
    return cache


def lifespan_inference_runner_cache() -> SharedResources:
    try:
        return _INFERENCE_RUNNERS.get()
    except LookupError as e:
        raise DependencyInjectionError("inference runner cache") from e


def set_postprocessor_cache(worker_config: ASRWorkerConfig) -> SharedResources:
    cache = worker_config.cache.postprocessor.to_resource_cache()
    _POSTPROCESSORS.set(cache)
    return cache


def lifespan_postprocessor_cache() -> SharedResources:
    try:
        return _POSTPROCESSORS.get()
    except LookupError as e:
        raise DependencyInjectionError("postprocessor cache") from e


@contextmanager
def set_multiprocessing_start_method(**_) -> Generator[None, None, None]:
    logger.info("setting multiprocessing start method...")
    multiprocessing.set_start_method("spawn", force=True)
    if platform.system() != "Darwin":
        logger.info("nothing to do !")
        yield
    else:
        old_method = multiprocessing.get_start_method()
        multiprocessing.set_start_method("fork", force=True)
        try:
            yield
        finally:
            multiprocessing.set_start_method(old_method, force=True)


REGISTRY = {
    "asr.inference": [
        set_worker_config,
        set_multiprocessing_start_method,
        set_inference_runner_cache,
    ],
    "asr.io": [set_worker_config, set_es_client],
    "asr.cpu": [set_worker_config, set_preprocessor_cache, set_postprocessor_cache],
}
