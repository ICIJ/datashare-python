from contextvars import ContextVar

from datashare_python.dependencies import (
    lifespan_es_client,  # noqa: F401
    set_es_client,
    set_loggers,
    set_worker_config,
)
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.utils import SharedResources

from .config import ExtractWorkerConfig

_EMBEDDER_CACHE: ContextVar[SharedResources] = ContextVar("embedder_cache")


def set_embedder_cache(worker_config: ExtractWorkerConfig) -> SharedResources:
    cache = worker_config.embedding.cache.to_resource_cache()
    _EMBEDDER_CACHE.set(cache)
    return cache


def lifespan_embedder_cache() -> SharedResources:
    try:
        return _EMBEDDER_CACHE.get()
    except LookupError as e:
        raise DependencyInjectionError("embedder cache") from e


IO = [set_worker_config, set_loggers, set_es_client]
EXTRACT = [set_worker_config, set_loggers]
EMBED = [set_worker_config, set_loggers, set_embedder_cache]

DEPENDENCIES = {"extract.io": IO, "extract.extract": EXTRACT, "extract.embed": EMBED}
