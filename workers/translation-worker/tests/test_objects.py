from typing import Any

import pytest
from datashare_python.conftest import TEST_PROJECT
from translation_worker.objects import (
    ArgosSentenceSplitterConfig,
    ArgosSentencizer,
    ArgosTranslatorConfig,
    DocId,
    DocumentSearchQuery,
    TranslationArgs,
    TranslationConfig,
)

from .conftest import DS_ENGLISH


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
    args = TranslationArgs(project=TEST_PROJECT, docs=docs, target_language=DS_ENGLISH)
    # When
    query = args.as_query()
    # Then
    assert query == expected_query
