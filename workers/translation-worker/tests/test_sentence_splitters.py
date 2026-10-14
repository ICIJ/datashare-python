# ruff: noqa: RUF001
import sys
from unittest.mock import MagicMock, patch

import pytest
from translation_worker.objects import (
    DEFAULT_SAT_MODEL_REF,
    DefaultSentenceSplitterConfig,
    RulesSentenceSplitterConfig,
    SaTSentenceSplitterConfig,
    TorchDevice,
)
from translation_worker.processors import cap_length
from translation_worker.sentence_splitters import (
    DefaultSentenceSplitter,
    RulesSentenceSplitter,
    SaTSentenceSplitter,
)

from tests.conftest import DS_ENGLISH


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", []),
        ("No terminator here", ["No terminator here"]),
        (
            "Hello world. How are you? I'm fine!",
            ["Hello world.", "How are you?", "I'm fine!"],
        ),
        # CJK terminators don't need a following space
        ("你好。今天天气很好！你呢？", ["你好。", "今天天气很好！", "你呢？"]),
        ("他说：“走吧。”然后离开了。", ["他说：“走吧。”", "然后离开了。"]),
        ("東京に行きました。楽しかった。", ["東京に行きました。", "楽しかった。"]),
        # closing quotes stay with their sentence
        ('He said "go." Then he left.', ['He said "go."', "Then he left."]),
        # decimals, urls
        (
            "It costs 3.5 dollars. See example.com now.",
            ["It costs 3.5 dollars.", "See example.com now."],
        ),  # noqa: E501
        # non-latin terminators
        ("यह एक वाक्य है। यह दूसरा है।", ["यह एक वाक्य है।", "यह दूसरा है।"]),
        ("یہ پہلا جملہ ہے۔ یہ دوسرا ہے۔", ["یہ پہلا جملہ ہے۔", "یہ دوسرا ہے۔"]),
        ("ပထမစာကြောင်း။ ဒုတိယစာကြောင်း။", ["ပထမစာကြောင်း။", "ဒုတိယစာကြောင်း။"]),
        ("ប្រយោគទីមួយ។ ប្រយោគទីពីរ។", ["ប្រយោគទីមួយ។", "ប្រយោគទីពីរ។"]),
        ("བཀྲ་ཤིས་བདེ་ལེགས། ཐུགས་རྗེ་ཆེ།", ["བཀྲ་ཤིས་བདེ་ལེགས།", "ཐུགས་རྗེ་ཆེ།"]),
        ("هل أنت بخير؟ نعم.", ["هل أنت بخير؟", "نعم."]),
        # words ending with a letter after a combining mark
        ("ते आले आहेत. ते गेले.", ["ते आले आहेत.", "ते गेले."]),
    ],
)
def test_rules_splitter_split_sentences(text: str, expected: list[str]) -> None:
    # Given
    splitter = RulesSentenceSplitter(RulesSentenceSplitterConfig())
    # When
    sentences = splitter.split_sentences(text)
    # Then
    assert sentences == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # known abbreviations
        ("Dr. Ehud Ur spoke. He left.", ["Dr. Ehud Ur spoke.", "He left."]),
        (
            "Prof. Kim und Hr. Weber kamen. Dann gingen sie.",
            ["Prof. Kim und Hr. Weber kamen.", "Dann gingen sie."],
        ),  # noqa: E501
        (
            "Он пришёл, ул. Ленина. Потом ушёл.",
            ["Он пришёл, ул. Ленина.", "Потом ушёл."],
        ),  # noqa: E501
        # initials
        (
            "John F. Kennedy spoke. Then he left.",
            ["John F. Kennedy spoke.", "Then he left."],
        ),  # noqa: E501
        # short dotted abbreviations
        (
            "Die Studie, d.h. Die Kontrolle, lief. Dann endete sie.",
            ["Die Studie, d.h. Die Kontrolle, lief.", "Dann endete sie."],
        ),  # noqa: E501
        ("The U.S. Army came. It left.", ["The U.S. Army came.", "It left."]),
        (
            "Он вернулся, т.е. Иван вернулся. Потом ушёл.",
            ["Он вернулся, т.е. Иван вернулся.", "Потом ушёл."],
        ),  # noqa: E501
        # lowercase continuation
        (
            "Studie, z.B. die Kontrolle. Dann nichts.",
            ["Studie, z.B. die Kontrolle.", "Dann nichts."],
        ),  # noqa: E501
        ("Wait... what happened? Nothing.", ["Wait... what happened?", "Nothing."]),
        ('"Really?" she asked. Yes.', ['"Really?" she asked.', "Yes."]),
        # words commonly ending sentences shouldn't be treated as abbreviations
        ("They said no. Then they left.", ["They said no.", "Then they left."]),
    ],
)
def test_rules_splitter_should_not_split_on_abbreviations(
    text: str, expected: list[str]
) -> None:
    # Given
    splitter = RulesSentenceSplitter(RulesSentenceSplitterConfig())
    # When
    sentences = splitter.split_sentences(text)
    # Then
    assert sentences == expected


@pytest.mark.parametrize(
    ("sentences", "max_length", "expected"),
    [
        (["a" * 20], None, ["a" * 20]),
        # short
        (["short one", "two"], 9, ["short one", "two"]),
        # prefers clause punctuation over whitespace
        (["aaa bbb, ccc ddd eee"], 14, ["aaa bbb,", "ccc ddd eee"]),
        (["一二三四五，六七八九十。"], 8, ["一二三四五，", "六七八九十。"]),
        # falls back to whitespace
        (["aaa bbb ccc ddd"], 10, ["aaa bbb", "ccc ddd"]),
        # ignores breaks in the first half of the window
        (["a, bbbbbbbbbbbb"], 8, ["a, bbbbb", "bbbbbbb"]),
        # hard cuts to text without breaks
        (["一二三四五六七八九十"], 4, ["一二三四", "五六七八", "九十"]),
    ],
)
def test_cap_length(
    sentences: list[str], max_length: int | None, expected: list[str]
) -> None:
    # When
    capped = cap_length(sentences, max_length)
    # Then
    assert capped == expected
    if max_length is not None:
        assert all(len(s) <= max_length for s in capped)


def test_splitter_caps_sentence_length() -> None:
    # Given
    config = DefaultSentenceSplitterConfig(max_length=10)
    splitter = DefaultSentenceSplitter(config)
    # When
    sentences = splitter.split_sentences("一二三四五六七八九十一二三")
    # Then
    assert sentences == ["一二三四五六七八九十", "一二三"]


def test_sat_splitter_split_sentences_batch_caps_sentence_length() -> None:
    # Given
    splitter = SaTSentenceSplitter(SaTSentenceSplitterConfig(max_length=4))
    splitter._sat = MagicMock()
    splitter._sat.split.return_value = iter([["一二三四五六"], ["七"]])
    # When
    sentences = splitter.split_sentences_batch(["一二三四五六", "七"])
    # Then
    assert sentences == [["一二三四", "五六"], ["七"]]


def _dummy_sat_modules(*, cuda_available: bool) -> tuple[dict, MagicMock]:
    torch = MagicMock()
    torch.cuda.is_available.return_value = cuda_available
    wtpsplit = MagicMock()
    return {"torch": torch, "wtpsplit": wtpsplit}, wtpsplit


@pytest.mark.parametrize(
    ("device", "cuda_available", "expected_on_gpu"),
    [
        (None, True, True),
        (None, False, False),
        (TorchDevice.CPU, True, False),
        (TorchDevice.GPU, True, True),
    ],
)
def test_sat_splitter_load(
    device: TorchDevice | None,
    cuda_available: bool,  # noqa: FBT001
    expected_on_gpu: bool,  # noqa: FBT001
) -> None:
    # Given
    config = SaTSentenceSplitterConfig(device=device)
    splitter = SaTSentenceSplitter(config)
    modules, wtpsplit = _dummy_sat_modules(cuda_available=cuda_available)
    # When
    with patch.dict(sys.modules, modules):
        splitter.load(DS_ENGLISH)
    # Then
    wtpsplit.SaT.assert_called_once_with(DEFAULT_SAT_MODEL_REF)
    sat = wtpsplit.SaT.return_value
    if expected_on_gpu:
        sat.half.return_value.to.assert_called_once_with(TorchDevice.GPU)
    else:
        sat.half.assert_not_called()


def test_sat_splitter_split_sentences_strips_and_drops_empty() -> None:
    # Given
    splitter = SaTSentenceSplitter(SaTSentenceSplitterConfig())
    splitter._sat = MagicMock()
    splitter._sat.split.return_value = ["ประโยคแรก ", " ", "ประโยคที่สอง"]
    # When
    sentences = splitter.split_sentences("ประโยคแรก ประโยคที่สอง")
    # Then
    assert sentences == ["ประโยคแรก", "ประโยคที่สอง"]


def test_sat_splitter_split_sentences_batch() -> None:
    # Given
    splitter = SaTSentenceSplitter(SaTSentenceSplitterConfig())
    splitter._sat = MagicMock()
    splitter._sat.split.return_value = iter([["a. ", "b."], ["c."]])
    # When
    sentences = splitter.split_sentences_batch(["a. b.", "c."])
    # Then
    splitter._sat.split.assert_called_once_with(["a. b.", "c."])
    assert sentences == [["a.", "b."], ["c."]]
