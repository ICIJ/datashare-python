from collections.abc import Callable, Iterable

from caul_core import ASRResult, BasePreprocessorConfig, Postprocessor
from icij_common.registrable import FromConfig

from .default import generic_unbatching_fn


class PostprocessorMixin(Postprocessor):
    """Postprocessing logic"""

    def __init__(self, unbatching_fn: Callable = generic_unbatching_fn):
        self._unbatching_fn = unbatching_fn

    @classmethod
    def _from_config(cls, config: BasePreprocessorConfig, **extras) -> FromConfig:
        return cls(**extras)

    def process(
        self, inputs: Iterable[ASRResult], *args, **kwargs
    ) -> Iterable[ASRResult]:
        """Unbatch batched inference results

        :param inputs: List of model results
        :return: list of model results in input ordering
        """
        yield from self._unbatching_fn(inputs)
