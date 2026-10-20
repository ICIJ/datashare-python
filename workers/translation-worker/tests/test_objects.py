from typing import Any

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import DatashareLanguage, Language
from translation_worker.objects import (
    ArgosSentenceSplitterConfig,
    ArgosSentencizer,
    ArgosTranslatorConfig,
    BaseTranslatorConfig,
    DefaultSentenceSplitterConfig,
    DocId,
    DocumentSearchQuery,
    HunyuanMtTranslatorConfig,
    RulesSentenceSplitterConfig,
    SaTSentenceSplitterConfig,
    SentenceBatcher,
    SentenceSplitterConfig,
    TranslationArgs,
    TranslationConfig,
)

from .conftest import DS_CHINESE, DS_ENGLISH


def test_config_deser() -> None:
    # Given
    config = {"sentence_splitter": {"model": "ARGOS"}, "translator": {"model": "ARGOS"}}

    # When
    deser = TranslationConfig.model_validate(config)
    # Then
    expected = TranslationConfig(
        sentence_splitter=ArgosSentenceSplitterConfig(
            sentencizer=ArgosSentencizer.MINI_SBD
        ),
        translator=ArgosTranslatorConfig(),
    )
    assert deser == expected


def test_config_deser_defaults() -> None:
    # When
    deser = TranslationConfig.model_validate({})
    # Then
    assert deser.sentence_splitter == RulesSentenceSplitterConfig()
    assert deser.language_sentence_splitters == {"th": SaTSentenceSplitterConfig()}
    assert deser.sentence_batcher == SentenceBatcher.BY_LENGTH


def test_config_deser_sentence_batcher() -> None:
    # Given
    config = {"sentenceBatcher": "IN_ORDER"}
    # When
    deser = TranslationConfig.model_validate(config)
    # Then
    assert deser.sentence_batcher == SentenceBatcher.IN_ORDER
    assert TranslationConfig.model_validate(deser.model_dump()) == deser


def test_config_deser_language_sentence_splitters() -> None:
    # Given
    config = {
        "languageSentenceSplitters": {
            "km": {"model": "SAT", "modelRef": "sat-12l-sm"},
            "GERMAN": {"model": "ARGOS"},
        }
    }
    # When
    deser = TranslationConfig.model_validate(config)
    # Then
    expected = {
        "km": SaTSentenceSplitterConfig(model_ref="sat-12l-sm"),
        "GERMAN": ArgosSentenceSplitterConfig(),
    }
    assert deser.language_sentence_splitters == expected
    assert TranslationConfig.model_validate(deser.model_dump()) == deser


@pytest.mark.parametrize(
    ("overrides", "language", "expected"),
    [
        (None, DatashareLanguage("THAI"), SaTSentenceSplitterConfig()),
        (None, "th", SaTSentenceSplitterConfig()),
        (None, DS_ENGLISH, RulesSentenceSplitterConfig()),
        (None, DS_CHINESE, RulesSentenceSplitterConfig()),
        (
            {"zh": DefaultSentenceSplitterConfig()},
            DS_CHINESE,
            DefaultSentenceSplitterConfig(),
        ),
        (
            {"zh": DefaultSentenceSplitterConfig()},
            "zh-Hant",
            DefaultSentenceSplitterConfig(),
        ),
        (
            {"ENGLISH": SaTSentenceSplitterConfig()},
            "en-US",
            SaTSentenceSplitterConfig(),
        ),
        ({}, DatashareLanguage("THAI"), RulesSentenceSplitterConfig()),
    ],
)
def test_config_sentence_splitter_for(
    overrides: dict | None, language: Language, expected: SentenceSplitterConfig
) -> None:
    # Given
    config = TranslationConfig()
    if overrides is not None:
        config = TranslationConfig(language_sentence_splitters=overrides)
    # When
    splitter_config = config.sentence_splitter_for(language)
    # Then
    assert splitter_config.model_copy(update={"max_length": None}) == expected


@pytest.mark.parametrize(
    ("splitter", "translator", "expected_max_length"),
    [
        (RulesSentenceSplitterConfig(), ArgosTranslatorConfig(), 1024),
        (RulesSentenceSplitterConfig(), HunyuanMtTranslatorConfig(), 1024),
        (
            RulesSentenceSplitterConfig(),
            HunyuanMtTranslatorConfig(max_new_tokens=512),
            256,
        ),
        # explicit lengths take precedence
        (
            RulesSentenceSplitterConfig(max_length=100),
            HunyuanMtTranslatorConfig(max_new_tokens=512),
            100,
        ),
    ],
)
def test_config_sentence_splitter_for_max_length(
    splitter: SentenceSplitterConfig,
    translator: BaseTranslatorConfig,
    expected_max_length: int,
) -> None:
    # Given
    config = TranslationConfig(sentence_splitter=splitter, translator=translator)
    # When
    splitter_config = config.sentence_splitter_for(DS_CHINESE)
    # Then
    assert splitter_config.max_length == expected_max_length


def test_config_sentence_splitter_for_language_max_length() -> None:
    # Given
    config = TranslationConfig(
        language_sentence_splitters={"zh": SaTSentenceSplitterConfig()},
        translator=HunyuanMtTranslatorConfig(max_new_tokens=300),
    )
    # When
    splitter_config = config.sentence_splitter_for(DS_CHINESE)
    # Then
    assert splitter_config == SaTSentenceSplitterConfig(max_length=150)


_NO_ES_TRANSLATED_CONTENT = {
    "bool": {
        "must_not": [
            {"term": {"content_translated.target_language.keyword": "ENGLISH"}}
        ]
    }
}


@pytest.mark.parametrize(
    ("docs", "expected_query"),
    [
        (None, _NO_ES_TRANSLATED_CONTENT),
        (["some-doc-id"], {"ids": {"values": ["some-doc-id"]}}),
        (
            {"term": {"contentType": "some-content-type"}},
            {"term": {"contentType": "some-content-type"}},
        ),
    ],
)
def test_translation_args_as_query(
    docs: list[DocId] | DocumentSearchQuery | None, expected_query: dict[str, Any]
) -> None:
    # Given
    args = TranslationArgs(
        project=TEST_PROJECT,
        docs=docs,
        target_language=DS_ENGLISH,
        config=TranslationConfig(),
    )
    # When
    query = args.as_query()
    # Then
    assert query == expected_query
