import logging
from _contextvars import ContextVar

from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    lifespan_worker_config,
    set_es_client,
    set_worker_config,
)
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.utils import SharedResources

from .config import TranslationWorkerConfig

logger = logging.getLogger(__name__)


_SENTENCE_SPLITTERS: ContextVar[SharedResources] = ContextVar("sentence_splitter_cache")
_TRANSLATORS: ContextVar[SharedResources] = ContextVar("translator_cache")


def set_sentence_splitter_cache(
    worker_config: TranslationWorkerConfig,
) -> SharedResources:
    cache = worker_config.cache.sentence_splitter.to_resource_cache()
    _SENTENCE_SPLITTERS.set(cache)
    return cache


def lifespan_sentence_splitter_cache() -> SharedResources:
    try:
        return _SENTENCE_SPLITTERS.get()
    except LookupError as e:
        raise DependencyInjectionError("sentence preprocessors cache") from e


def set_translator_cache(
    worker_config: TranslationWorkerConfig,
) -> SharedResources:
    cache = worker_config.cache.translator.to_resource_cache()
    _TRANSLATORS.set(cache)
    return cache


def lifespan_translator_cache() -> SharedResources:
    try:
        return _TRANSLATORS.get()
    except LookupError as e:
        raise DependencyInjectionError("translators cache") from e


REGISTRY = {
    "translation.inference": [
        set_worker_config,
        set_es_client,
        set_sentence_splitter_cache,
        set_translator_cache,
    ],
    "translation.io": [set_worker_config, set_es_client],
}
