import gc
from typing import Self

from datashare_python.objects import DatashareLanguage
from pydantic_extra_types.language_code import LanguageAlpha2

from .config import ArgosSentenceSplitterConfig, DefaultSentenceSplitterConfig
from .objects import Language, SentenceSplitterModel
from .processors import SentenceSplitter


@SentenceSplitter.register(SentenceSplitterModel.DEFAULT)
class DefaultSentenceSplitter(SentenceSplitter):
    def __init__(self, config: DefaultSentenceSplitterConfig):
        self._config = config

    def load(self, language: "Language") -> Self: ...

    @classmethod
    def _from_config(cls, config: DefaultSentenceSplitterConfig) -> Self:
        return cls(config)

    def split_sentences(self, text: str) -> list[str]:
        return [text]


@SentenceSplitter.register(SentenceSplitterModel.ARGOS)
class ArgosSentenceSplitter(SentenceSplitter):
    def __init__(self, config: ArgosSentenceSplitterConfig):
        self._config = config
        self._inner = None

    def load(self, language: Language) -> Self:
        from argostranslate.package import get_installed_packages  # noqa: PLC0415

        if isinstance(language, DatashareLanguage):
            language = LanguageAlpha2(language.alpha2)

        installed = get_installed_packages()
        p = next((p for p in installed if p.from_code == language), None)
        if p is None:
            msg = (
                f"unknown language: {language}, install the translation package first"
                f" in order to make the sbd package available"
            )
            raise ValueError(msg)  # noqa: PLC0415
        self._inner = self._config.sentencizer.sentencizer_cls(p)

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        del self._inner
        gc.collect()

    def split_sentences(self, text: str) -> list[str]:
        return self._inner.split_sentences(text)

    @classmethod
    def _from_config(cls, config: ArgosSentenceSplitterConfig) -> Self:
        return cls(config)
