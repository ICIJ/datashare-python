from translation_worker.objects import (
    ArgosSentenceSplitterConfig,
    ArgosSentencizer,
    ArgosTranslatorConfig,
    TranslationConfig,
)


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
