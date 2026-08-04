from contextvars import ContextVar

from datashare_python.dependencies import set_es_client, set_loggers, set_worker_config
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.utils import SharedResources

from passport_worker.config import PassportWorkerConfig

_IMAGE_PREPROCESSOR_CACHE: ContextVar[SharedResources] = ContextVar(
    "image_preprocessor_cache"
)
_PDF_CONVERTER_CACHE: ContextVar[SharedResources] = ContextVar("pdf_converter_cache")
_PASSPORT_DETECTOR_CACHE: ContextVar[SharedResources] = ContextVar("preprocessor_cache")


def set_image_preprocessor_cache(
    worker_config: PassportWorkerConfig,
) -> SharedResources:
    cache = worker_config.cache.preprocessing.images.to_resource_cache()
    _IMAGE_PREPROCESSOR_CACHE.set(cache)
    return cache


def lifespan_image_preprocessor_cache() -> SharedResources:
    try:
        return _IMAGE_PREPROCESSOR_CACHE.get()
    except LookupError as e:
        raise DependencyInjectionError("image preprocessor") from e


def set_pdf_converter_cache(
    worker_config: PassportWorkerConfig,
) -> SharedResources:
    cache = worker_config.cache.preprocessing.pdf.to_resource_cache()
    _PDF_CONVERTER_CACHE.set(cache)
    return cache


def lifespan_pdf_converter_cache() -> SharedResources:
    try:
        return _PDF_CONVERTER_CACHE.get()
    except LookupError as e:
        raise DependencyInjectionError("pdf converter cache") from e


def set_passport_detector_cache(worker_config: PassportWorkerConfig) -> SharedResources:
    cache = worker_config.cache.inference.to_resource_cache()
    _PASSPORT_DETECTOR_CACHE.set(cache)
    return cache


def lifespan_passport_detector_cache() -> SharedResources:
    try:
        return _PASSPORT_DETECTOR_CACHE.get()
    except LookupError as e:
        raise DependencyInjectionError("passport detector cache") from e


IO = [set_worker_config, set_loggers, set_es_client, set_pdf_converter_cache]
PREPROCESSING = [set_worker_config, set_loggers, set_image_preprocessor_cache]
INFERENCE = [set_worker_config, set_loggers, set_passport_detector_cache]

DEPENDENCIES = {
    "passport-detection.io": IO,
    "passport-detection.preprocessing": PREPROCESSING,
    "passport-detection.inference": INFERENCE,
}
