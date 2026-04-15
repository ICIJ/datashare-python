import multiprocessing
import platform
from collections.abc import Generator
from contextlib import contextmanager

from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    lifespan_worker_config,
    set_es_client,
    set_worker_config,
)


@contextmanager
def set_multiprocessing_start_method(**_) -> Generator[None, None, None]:
    if platform.system() != "Darwin":
        yield
        return
    old_method = multiprocessing.get_start_method()
    multiprocessing.set_start_method("fork", force=True)
    try:
        yield
    finally:
        multiprocessing.set_start_method(old_method, force=True)


REGISTRY = {
    "inference": [set_worker_config, set_multiprocessing_start_method],
    "io": [set_worker_config, set_es_client],
    "preprocessing": [set_worker_config],
}
