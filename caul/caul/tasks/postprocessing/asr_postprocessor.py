from collections.abc import Callable, Iterable
from typing import Self

from caul_core import ASRResult, BasePreprocessorConfig, Postprocessor

from .default import generic_unbatching_fn


class PostprocessorMixin(Postprocessor):
    """Postprocessing logic"""

    def __init__(self, unbatching_fn: Callable = generic_unbatching_fn):
        self._unbatching_fn = unbatching_fn

    @classmethod
    def _from_config(cls, config: BasePreprocessorConfig, **extras) -> Self:  # noqa: ARG003
        return cls(**extras)

    def process(
        self,
        inputs: Iterable[ASRResult],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult]:
        """Unbatch batched inference results

        :param inputs: List of model results
        :return: list of model results in input ordering
        """
        yield from self._unbatching_fn(inputs)
