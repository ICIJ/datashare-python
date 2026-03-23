from _contextvars import ContextVar

from datashare_python.exceptions import DependencyInjectionError

from .config_ import NERWorkerConfig
from .spacy_.ner import SpacyPipelineCache

SPACY_PIPELINE_CACHE: ContextVar[SpacyPipelineCache] = ContextVar(
    "spacy_pipeline_cache"
)


def setup_spacy_pipeline_cache(
    worker_config: NERWorkerConfig, **_
) -> SpacyPipelineCache:
    pipeline_cache = worker_config.to_spacy_pipeline_cache()
    SPACY_PIPELINE_CACHE.set(pipeline_cache)
    return pipeline_cache


def lifespan_spacy_pipeline_cache() -> SpacyPipelineCache:
    try:
        return SPACY_PIPELINE_CACHE.get()
    except LookupError as e:
        raise DependencyInjectionError("spacy pipeline cache") from e
