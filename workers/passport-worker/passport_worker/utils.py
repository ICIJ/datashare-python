import logging
from collections.abc import Callable
from functools import wraps
from inspect import iscoroutinefunction
from typing import Protocol

from datashare_python.objects import FromException, ProcessedFile

logger = logging.getLogger(__name__)


class _PreprocessingFunction[R](Protocol):
    def __call__(self, doc: ProcessedFile, *args, **kwargs) -> R: ...


def reports_errors[R, E: FromException](
    errors: tuple[type[Exception]], exc_cls: type[E]
) -> Callable[[_PreprocessingFunction[R]], _PreprocessingFunction[R | E]]:

    def parent_wrapper(
        f: _PreprocessingFunction[R],
    ) -> _PreprocessingFunction[R | E]:
        if iscoroutinefunction(f):

            @wraps(f)  # noqa: F821
            async def wrapper(doc: ProcessedFile, *args, **kwargs) -> R | E:
                try:
                    return await f(doc, *args, **kwargs)
                except errors as e:
                    logger.exception("error while processing doc %s", doc)
                    report = exc_cls.from_exception(doc, e)
                    return report
        else:

            @wraps(f)
            def wrapper(doc: ProcessedFile, *args, **kwargs) -> R | E:

                try:
                    return f(doc, *args, **kwargs)
                except errors as e:
                    logger.exception("error while processing doc %s", doc)
                    report = exc_cls.from_exception(doc, e)
                    return report

        return wrapper

    return parent_wrapper
