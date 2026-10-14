import logging
import re
import sys
from abc import abstractmethod
from collections.abc import Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self, final

from icij_common.registrable import RegistrableFromConfig

from .config import TranslationWorkerConfig

if TYPE_CHECKING:
    from .config import BaseTranslatorConfig
    from .objects import Language, SentenceSplitterConfig

logger = logging.getLogger(__name__)


_CLAUSE_BREAK = re.compile(r"[,;:，；：、]+")
_WHITESPACE = re.compile(r"\s+")


def cap_length(sentences: list[str], max_length: int | None) -> list[str]:
    """Cut sentences longer than ``max_length`` characters into chunks.

    Chunks end at last punctuation or whitespace. Sentences without either
    are split at exactly ``max_length`` characters as a fallback.

    :param sentences: split sentences
    :param max_length: maximum number of characters per sentence or ``None`` for no cap
    :return: sentences no longer than ``max_length`` characters
    """
    if max_length is None:
        return sentences
    return [chunk for s in sentences for chunk in _cap_sentence_length(s, max_length)]


def _cap_sentence_length(sentence: str, max_length: int) -> Iterable[str]:
    while len(sentence) > max_length:
        cut = _break_position(sentence, max_length)
        chunk, sentence = sentence[:cut].strip(), sentence[cut:].strip()
        if chunk:
            yield chunk
    if sentence:
        yield sentence


def _break_position(sentence: str, max_length: int) -> int:
    min_cut = max_length // 2
    for pattern in (_CLAUSE_BREAK, _WHITESPACE):
        breaks = list(pattern.finditer(sentence, min_cut, max_length))
        if breaks:
            return breaks[-1].end()
    return max_length


class SentenceSplitter(RegistrableFromConfig):
    def __init__(self, config: "SentenceSplitterConfig"):
        self._config = config

    @final
    def split_sentences(self, text: str) -> list[str]:
        return cap_length(self._split_sentences(text), self._config.max_length)

    @final
    def split_sentences_batch(self, batch: list[str]) -> list[list[str]]:
        return [
            cap_length(sentences, self._config.max_length)
            for sentences in self._split_sentences_batch(batch)
        ]

    @abstractmethod
    def _split_sentences(self, text: str) -> list[str]: ...

    def _split_sentences_batch(self, batch: list[str]) -> list[list[str]]:
        return [self._split_sentences(t) for t in batch]

    @final
    @contextmanager
    def load_cm(self, language: "Language") -> Self:
        self.load(language)
        try:
            yield self
        finally:
            self.__exit__(*sys.exc_info())

    @abstractmethod
    def load(self, language: "Language") -> Self: ...

    @final
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): ...  # noqa: ANN001


class Translator(RegistrableFromConfig):
    def __init__(self, config: "BaseTranslatorConfig"):
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
    def load_cm(
        self,
        source: "Language",
        *,
        target: "Language",
        worker_config: TranslationWorkerConfig | None = None,
    ) -> Self:
        if worker_config is None:
            worker_config = TranslationWorkerConfig()
        self.load(source, target=target, worker_config=worker_config)
        try:
            yield self
        finally:
            self.__exit__(*sys.exc_info())

    def load(
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
