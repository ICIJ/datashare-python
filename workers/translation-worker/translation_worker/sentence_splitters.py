import gc
import re
from typing import Self

from datashare_python.objects import DatashareLanguage
from pydantic_extra_types.language_code import LanguageAlpha2

from .objects import (
    ArgosSentenceSplitterConfig,
    DefaultSentenceSplitterConfig,
    Language,
    RulesSentenceSplitterConfig,
    SaTSentenceSplitterConfig,
    SentenceSplitterModel,
    TorchDevice,
)
from .processors import SentenceSplitter

_CLOSING = "\"'”’」』）)»›\u200b"
_TERMINATOR = re.compile(rf"[。！？]+[{_CLOSING}]*|[.!?…។။།༎।॥۔؟|]+[{_CLOSING}]*(?=\s)")
_CASED_TERMINATORS = ".!?…"
_NEXT_CHAR = re.compile(r"\s*(\S)")
_PREVIOUS_TOKEN = re.compile(r"\w+(?:\.\w+)*$")
_MAX_ABBREVIATION_LEN = 16
_DOTTED_ABBREVIATION = re.compile(r"(?:\w{1,2}\.)+\w{1,2}")
_ABBREVIATIONS = frozenset(
    abbreviation
    for abbreviations in (
        # en
        "mr mrs ms dr prof sr jr st mt vs fig nr vol approx dept inc ltd corp gen gov"
        " rep lt col capt sgt rev hon",
        # de
        "hr frl bzw ggf vgl evtl ca mio mrd tel str abs bd jh jhd",
        # fr, es, pt, it, nl
        "mme mlle mgr sra srta dra ud uds av avda pág núm sig sigg dott ing avv dhr"
        " mevr bijv blz",
        # pl, cs, tr, id, ms, tl
        "np tzw tj ul al inż godz św např tzv doç bkz sdr bpk jl hlm gng bb",
        # ru, uk, kk, mn
        "г гг др им ул стр см рис англ тыс млн млрд руб ст ім вул тис грн",
    )
    for abbreviation in abbreviations.split()
)


def _is_abbreviation(text: str, end: int) -> bool:
    start = max(0, end - _MAX_ABBREVIATION_LEN)
    token = _PREVIOUS_TOKEN.search(text, start, end)
    if token is None:
        return False
    token = token.group()
    if len(token) == 1 and token.isupper():
        return True
    return (
        _DOTTED_ABBREVIATION.fullmatch(token) is not None
        or token.casefold() in _ABBREVIATIONS
    )


def _is_sentence_end(text: str, terminator: re.Match) -> bool:
    if terminator.group()[0] not in _CASED_TERMINATORS:
        return True
    next_char = _NEXT_CHAR.match(text, terminator.end())
    if next_char is not None and next_char.group(1).islower():
        return False
    if terminator.group()[0] == ".":
        return not _is_abbreviation(text, terminator.start())
    return True


def _clean(sentences: list[str]) -> list[str]:
    return [s for s in (s.strip() for s in sentences) if s]


@SentenceSplitter.register(SentenceSplitterModel.DEFAULT)
class DefaultSentenceSplitter(SentenceSplitter):
    def load(self, language: "Language") -> Self: ...

    @classmethod
    def _from_config(cls, config: DefaultSentenceSplitterConfig) -> Self:
        return cls(config)

    def _split_sentences(self, text: str) -> list[str]:
        return [text]


@SentenceSplitter.register(SentenceSplitterModel.ARGOS)
class ArgosSentenceSplitter(SentenceSplitter):
    def __init__(self, config: ArgosSentenceSplitterConfig):
        super().__init__(config)
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

    def _split_sentences(self, text: str) -> list[str]:
        return self._inner.split_sentences(text)

    @classmethod
    def _from_config(cls, config: ArgosSentenceSplitterConfig) -> Self:
        return cls(config)


@SentenceSplitter.register(SentenceSplitterModel.RULES)
class RulesSentenceSplitter(SentenceSplitter):
    """Whitespace and punctuation splitter. Languages that don't rely on punctuation
    (e.g. Thai) should use a different approach.
    """

    def load(self, language: Language) -> Self:  # noqa: ARG002
        return self

    @classmethod
    def _from_config(cls, config: RulesSentenceSplitterConfig) -> Self:
        return cls(config)

    def _split_sentences(self, text: str) -> list[str]:
        sentences, start = [], 0
        for terminator in _TERMINATOR.finditer(text):
            if _is_sentence_end(text, terminator):
                sentences.append(text[start : terminator.end()])
                start = terminator.end()
        sentences.append(text[start:])
        return _clean(sentences)


@SentenceSplitter.register(SentenceSplitterModel.SAT)
class SaTSentenceSplitter(SentenceSplitter):
    """Segment any Text (SaT) splitter"""

    def __init__(self, config: SaTSentenceSplitterConfig):
        super().__init__(config)
        self._sat = None

    def load(self, language: Language) -> Self:  # noqa: ARG002
        import torch  # noqa: PLC0415
        from wtpsplit import SaT  # noqa: PLC0415

        sat = SaT(self._config.model_ref)
        device = self._config.device
        if device is None:
            device = TorchDevice.GPU if torch.cuda.is_available() else TorchDevice.CPU
        if device is TorchDevice.GPU:
            sat.half().to(device)
        self._sat = sat
        return self

    @classmethod
    def _from_config(cls, config: SaTSentenceSplitterConfig) -> Self:
        return cls(config)

    def _split_sentences(self, text: str) -> list[str]:
        return _clean(self._sat.split(text))

    def _split_sentences_batch(self, batch: list[str]) -> list[list[str]]:
        return [_clean(sentences) for sentences in self._sat.split(batch)]

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        import torch  # noqa: PLC0415

        self._sat = None
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        gc.collect()
