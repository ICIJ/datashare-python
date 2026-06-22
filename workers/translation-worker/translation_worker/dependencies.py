import gc
import logging
from contextlib import asynccontextmanager
from typing import Any

from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    lifespan_shared_resources,
    lifespan_worker_config,
    set_es_client,
    set_shared_resources,
    set_worker_config,
)
from datashare_python.exceptions import DependencyInjectionError
from datashare_python.objects import Shared

from .constants import HUNYUAN_SHARED_KEY

logger = logging.getLogger(__name__)


@asynccontextmanager
async def set_hunyuan_translator(**kwargs) -> None:  # noqa: ARG001
    shared = Shared()
    await set_shared_resources(shared)
    try:
        yield
    finally:
        import torch  # noqa: PLC0415

        model = await shared.async_pop_resource(HUNYUAN_SHARED_KEY, None)
        if model is not None:
            del model
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            gc.collect()


def lifespan_hunyuan_translator() -> Any:
    try:
        return lifespan_shared_resources().get_resource(HUNYUAN_SHARED_KEY)
    except (LookupError, KeyError) as e:
        raise DependencyInjectionError("hunyuan translator") from e


REGISTRY = {
    "translation.inference": [set_worker_config, set_es_client, set_hunyuan_translator],
    "translation.io": [set_worker_config, set_es_client],
}
