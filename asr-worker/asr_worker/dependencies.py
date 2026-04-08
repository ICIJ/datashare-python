import multiprocessing
import platform
from collections.abc import Generator
from contextlib import contextmanager


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


REGISTRY = {"INFERENCE": [set_multiprocessing_start_method]}
