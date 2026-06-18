import logging
from abc import abstractmethod
from collections.abc import Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self, final

from icij_common.registrable import RegistrableFromConfig

from .config import TranslationWorkerConfig

if TYPE_CHECKING:
    from .config import TranslatorConfig
    from .objects import Language

logger = logging.getLogger(__name__)


class SentenceSplitter(RegistrableFromConfig):
    @abstractmethod
    def split_sentences(self, text: str) -> list[str]: ...  # noqa: F821

    def split_sentences_batch(self, batch: list[str]) -> list[list[str]]:
        return [self.split_sentences(t) for t in batch]

    @final
    @contextmanager
    def load(self, language: "Language") -> Self:
        with self:
            self._load(language)
            yield self

    def _load(self, language: "Language") -> Self: ...

    @final
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001


class Translator(RegistrableFromConfig):
    def __init__(self, config: "TranslatorConfig"):
        self._config = config

        self._source: Language | None = None
        self._target: Language | None = None

    @abstractmethod
    def translate(self, texts: Iterable[str]) -> list[str]: ...  # noqa: F821

    @property
    def source(self) -> "Language":
        if self._source is None:
            raise ValueError("translator has no source language as it was not loaded")
        return self._source

    @property
    def target(self) -> "Language":
        if self._target is None:
            raise ValueError("translator has no target language as it was not loaded")
        return self._target

    @contextmanager
    @final
    def load(
        self,
        source: "Language",
        *,
        target: "Language",
        worker_config: TranslationWorkerConfig | None = None,
    ) -> Self:
        if worker_config is None:
            worker_config = TranslationWorkerConfig()
        with self:
            self._load(source, target=target, worker_config=worker_config)
            yield self

    def _load(
        self,
        source: "Language",
        *,
        target: "Language",
        worker_config: TranslationWorkerConfig,  # noqa: ARG002
    ) -> None:
        self._source = source
        self._target = target

    @final
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        self._source = None
        self._target = None
