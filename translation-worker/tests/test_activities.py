# ruff: noqa: ARG001, ANN001, ANN202, FBT001, FBT002

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from icij_common.es import (
    BOOL,
    COUNT,
    DOC_CONTENT,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    HITS,
    ID_,
    MUST_NOT,
    SOURCE,
    TERM,
)
from translation_worker.activities import (
    _add_translation,
    _collect_language_batches,
    _count_untranslated,
    _get_untranslated,
    _iter_sentences,
    _process_language_group,
    _translate_batch,
    _untranslated_by_language,
    _untranslated_query,
    create_translation_batches,
    translate_sentences,
)
from translation_worker.objects import (
    BatchSentence,
    SentencesBatch,
    TranslationConfig,
)

from tests.conftest import (
    DOC_ID_1,
    DOC_ID_2,
    EN,
    ENGLISH,
    ES,
    FR,
    FRENCH,
    MOCK_TRANSLATIONS,
    ROOT_DOCUMENT_1,
    ROOT_DOCUMENT_2,
    SPANISH,
    TEST_PROJECT,
)


def _make_es_doc(doc_id: str, content: str, language: str, root_document: str) -> dict:
    return {
        ID_: doc_id,
        SOURCE: {
            DOC_CONTENT: content,
            DOC_LANGUAGE: language,
            DOC_ROOT_ID: root_document,
        },
    }


def _make_sentences_batch_dict(
    n_sentences: int = 2, same_doc: bool = False, language: str = FR
) -> dict:
    sentences = [
        {
            "doc_id": DOC_ID_1 if same_doc else f"doc_id_{i}",
            "root_document": ROOT_DOCUMENT_1 if same_doc else f"root_document_{i}",
            "sentence_index": i,
            "sentence": f"{i}",
        }
        for i in range(n_sentences)
    ]
    return {"language": language, "sentences": sentences}


async def _collect_async(gen: AsyncGenerator) -> list:
    return [item async for item in gen]


async def _empty_docs_iter(*args, **kwargs) -> AsyncGenerator[None, Any]:
    return
    yield


async def _single_doc_iter(es_doc: dict) -> AsyncGenerator:
    yield es_doc


EN_DOC_1_TEXT = "Hello"
EN_DOC_2_TEXT = "Goodbye"
FR_DOC_1_TEXT = "Bonjour"
FR_DOC_2_TEXT = "Au revoir"
EN_DOC_1 = _make_es_doc(DOC_ID_1, EN_DOC_1_TEXT, EN, ROOT_DOCUMENT_1)
EN_DOC_2 = _make_es_doc(DOC_ID_2, EN_DOC_2_TEXT, EN, ROOT_DOCUMENT_2)
FR_DOC_1 = es_doc = _make_es_doc(DOC_ID_1, FR_DOC_1_TEXT, FR, ROOT_DOCUMENT_1)


# _untranslated_query


def test__untranslated_query__fields() -> None:
    query = _untranslated_query(EN)

    assert "query" in query
    assert BOOL in query["query"]
    assert "content_translated.en" in str(query)


def test__untranslated_query__comparison() -> None:
    assert _untranslated_query(EN) != _untranslated_query(FR)


def test__untranslated_query__doc_lang_other_than_english() -> None:
    assert _untranslated_query(FR)["query"][BOOL][MUST_NOT][1][TERM][DOC_LANGUAGE] == FR


# _iter_sentences


async def test__iter_sentences__yields_one_batch_per_doc_with_correct_indices() -> None:
    es_doc = _make_es_doc(
        DOC_ID_1, "Hello world. How are you? I'm fine.", EN, ROOT_DOCUMENT_1
    )

    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [
        "Hello world.",
        "How are you?",
        "I'm fine.",
    ]
    translation_package = MagicMock()
    translation_package.sentencizer = sentencizer

    batches = await _collect_async(
        _iter_sentences(_single_doc_iter(es_doc), translation_package, EN)
    )

    assert len(batches) == 1
    assert batches[0].language == EN
    assert len(batches[0].sentences) == 3
    assert [s.sentence_index for s in batches[0].sentences] == [0, 1, 2]


async def test__iter_sentences__doc_id_and_root_document_are_preserved() -> None:
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [FR_DOC_1_TEXT]
    translation_package = MagicMock()
    translation_package.sentencizer = sentencizer

    batches = await _collect_async(
        _iter_sentences(_single_doc_iter(FR_DOC_1), translation_package, FR)
    )

    sentence = batches[0].sentences[0]
    assert sentence.doc_id == DOC_ID_1
    assert sentence.root_document == ROOT_DOCUMENT_1


async def test__iter_sentences__multiple_docs_yield_multiple_batches() -> None:
    async def docs_iter():
        yield EN_DOC_1
        yield EN_DOC_2

    sentencizer = MagicMock()
    sentencizer.split_sentences.side_effect = [[EN_DOC_1_TEXT], [EN_DOC_2_TEXT]]
    translation_package = MagicMock()
    translation_package.sentencizer = sentencizer

    batches = await _collect_async(
        _iter_sentences(docs_iter(), translation_package, EN)
    )

    assert len(batches) == 2


async def test__iter_sentences__empty_docs_iter_yields_nothing() -> None:
    batches = await _collect_async(_iter_sentences(_empty_docs_iter(), MagicMock(), EN))

    assert batches == []


# _collect_language_batches


async def test__collect_language_batches__collects_batches_into_list() -> None:
    expected = [
        SentencesBatch(language=FR, sentences=[]),
        SentencesBatch(language=FR, sentences=[]),
    ]

    async def mock_process(*args, **kwargs):
        for batch in expected:
            yield batch

    with patch(
        "translation_worker.activities._process_language_group",
        side_effect=mock_process,
    ):
        result = await _collect_language_batches(MagicMock(), EN, TranslationConfig())

    assert result == expected


async def test__collect_language_batches__returns_empty_list_when_no_batches() -> None:
    with patch(
        "translation_worker.activities._process_language_group",
        side_effect=_empty_docs_iter,
    ):
        result = await _collect_language_batches(MagicMock(), EN, TranslationConfig())

    assert result == []


# _process_language_group


async def test__collect_language_batches__yields_sentences_batches() -> None:
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [FR_DOC_1_TEXT]
    mock_package = MagicMock()
    mock_package.sentencizer = sentencizer

    language_docs_iter = MagicMock()
    language_docs_iter.language = FR
    language_docs_iter.document_iter = _single_doc_iter(FR_DOC_1)

    with patch(
        "translation_worker.activities._get_translation_package",
        return_value=mock_package,
    ):
        batches = await _collect_async(
            _process_language_group(language_docs_iter, EN, TranslationConfig())
        )

    assert len(batches) == 1
    assert batches[0].language == FR
    assert len(batches[0].sentences) == 1


async def test__collect_language_batches__empty_docs_yields_nothing() -> None:
    language_docs_iter = MagicMock()
    language_docs_iter.language = FR
    language_docs_iter.document_iter = _empty_docs_iter()

    with patch(
        "translation_worker.activities._get_translation_package",
        return_value=MagicMock(),
    ):
        batches = await _collect_async(
            _process_language_group(language_docs_iter, EN, TranslationConfig())
        )

    assert batches == []


# create_translation_batches


async def test__create_translation_batches__returns_flat_list_of_batches() -> None:
    batch1 = SentencesBatch(language=FR, sentences=[])
    batch2 = SentencesBatch(language=ES, sentences=[])

    lang_groups = [MagicMock(), MagicMock()]

    async def mock_untranslated_by_language(*args, **kwargs):
        for g in lang_groups:
            yield g

    async def mock_collect_language_batches(language_docs, *args, **kwargs):
        if language_docs is lang_groups[0]:
            return [batch1]
        return [batch2]

    with (
        patch(
            "translation_worker.activities._untranslated_by_language",
            side_effect=mock_untranslated_by_language,
        ),
        patch(
            "translation_worker.activities._collect_language_batches",
            side_effect=mock_collect_language_batches,
        ),
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language_alpha_code=EN,
            config=TranslationConfig(),
        )

    assert len(result) == 2
    assert batch1 in result
    assert batch2 in result


async def test__create_translation_batches__returns_empty_list_when_no_docs() -> None:
    with patch(
        "translation_worker.activities._untranslated_by_language",
        side_effect=_empty_docs_iter,
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language_alpha_code=EN,
            config=TranslationConfig(),
        )

    assert result == []


# translate_sentences


async def test_translate_sentences__returns_zero_for_empty_batch() -> None:
    empty_batch = {"language": FR, "sentences": []}
    result = await translate_sentences(
        empty_batch,
        EN,
        project=TEST_PROJECT,
        es_client=MagicMock(),
        config=TranslationConfig(),
    )
    assert result == 0


async def test_translate_sentences__accepts_none_config_and_defaults() -> None:
    empty_batch = {"language": FR, "sentences": []}
    result = await translate_sentences(
        empty_batch,
        EN,
        project=TEST_PROJECT,
        es_client=MagicMock(),
        config=None,
    )
    assert result == 0


async def test_translate_sentences__returns_count_of_translated_docs() -> None:
    batch_dict = _make_sentences_batch_dict(n_sentences=2, same_doc=False)

    async def mock_translate_batch(*args, **kwargs):
        return [EN_DOC_1_TEXT, EN_DOC_2_TEXT]

    with (
        patch(
            "translation_worker.activities._translate_batch",
            side_effect=mock_translate_batch,
        ),
        patch(
            "translation_worker.activities._add_translation",
            new_callable=AsyncMock,
        ),
    ):
        result = await translate_sentences(
            batch_dict,
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            config=TranslationConfig(),
        )

    assert result == 2


async def test_translate_sentences__sentences_from_same_doc_count_as_one() -> None:
    batch_dict = _make_sentences_batch_dict(n_sentences=2, same_doc=True)

    async def mock_translate_batch(*args, **kwargs):
        return [FR_DOC_1_TEXT, FR_DOC_2_TEXT]

    with (
        patch(
            "translation_worker.activities._translate_batch",
            side_effect=mock_translate_batch,
        ),
        patch(
            "translation_worker.activities._add_translation",
            new_callable=AsyncMock,
        ),
    ):
        result = await translate_sentences(
            batch_dict,
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            config=TranslationConfig(),
        )

    assert result == 1


async def test_translate_sentences__reconstructs_translation_in_sentence_order() -> (
    None
):
    sentences = [
        {
            "doc_id": DOC_ID_1,
            "root_document": ROOT_DOCUMENT_1,
            "sentence_index": 0,
            "sentence": EN_DOC_1_TEXT,
        },
        {
            "doc_id": DOC_ID_1,
            "root_document": ROOT_DOCUMENT_1,
            "sentence_index": 1,
            "sentence": EN_DOC_2_TEXT,
        },
    ]
    batch_dict = {"language": FR, "sentences": sentences}
    captured_translations = []

    async def mock_translate_batch(*args, **kwargs) -> None:
        return [FR_DOC_1_TEXT, FR_DOC_2_TEXT]

    async def mock_add_translation(
        es_client, translations, project, *, target_language_alpha_code
    ) -> None:
        captured_translations.extend(translations)

    with (
        patch(
            "translation_worker.activities._translate_batch",
            side_effect=mock_translate_batch,
        ),
        patch(
            "translation_worker.activities._add_translation",
            side_effect=mock_add_translation,
        ),
    ):
        await translate_sentences(
            batch_dict,
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            config=TranslationConfig(),
        )

    assert len(captured_translations) == 1
    _, _, combined = captured_translations[0]
    assert FR_DOC_1_TEXT in combined
    assert FR_DOC_2_TEXT in combined
    assert combined.index(FR_DOC_1_TEXT) < combined.index(FR_DOC_2_TEXT)


# _translate_batch


async def test__translate_batch_returns_translations_from_translate_as_list() -> None:
    sentences = (
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=0,
            sentence=EN_DOC_1_TEXT,
        ),
    )
    with patch(
        "translation_worker.activities._translate_as_list",
        return_value=[FR_DOC_1_TEXT],
    ):
        result = await _translate_batch(sentences, EN, FR, TranslationConfig())

    assert result == [FR_DOC_1_TEXT]


# _untranslated_by_language


async def test__untranslated_by_language__yields_one_group_per_language() -> None:
    async def mock_get_untranslated(*args, **kwargs):
        yield _make_es_doc(DOC_ID_1, FR_DOC_1_TEXT, FRENCH, ROOT_DOCUMENT_1)
        yield _make_es_doc(DOC_ID_2, "Hola", SPANISH, ROOT_DOCUMENT_2)

    def mock_language_alpha_codes(*languages: str):
        return [FR] if FRENCH in languages else [ES]

    with (
        patch(
            "translation_worker.activities._get_untranslated",
            side_effect=mock_get_untranslated,
        ),
        patch(
            "translation_worker.activities._language_alpha_codes",
            side_effect=mock_language_alpha_codes,
        ),
    ):
        groups = []
        async for group in _untranslated_by_language(MagicMock(), TEST_PROJECT, EN):
            # Consume iterator so the outer loop can advance
            docs = [doc async for doc in group.document_iter]
            groups.append((group.language, docs))

    assert len(groups) == 2
    assert groups[0][0] == FR
    assert groups[1][0] == ES


async def test__untranslated_by_language__groups_docs_of_same_language_together() -> (
    None
):
    async def mock_get_untranslated(*args, **kwargs):
        yield _make_es_doc(DOC_ID_1, EN_DOC_1_TEXT, ENGLISH, ROOT_DOCUMENT_1)
        yield _make_es_doc(DOC_ID_2, EN_DOC_2_TEXT, ENGLISH, ROOT_DOCUMENT_2)

    with (
        patch(
            "translation_worker.activities._get_untranslated",
            side_effect=mock_get_untranslated,
        ),
        patch(
            "translation_worker.activities._language_alpha_codes",
            return_value=[EN],
        ),
    ):
        groups = []
        async for group in _untranslated_by_language(MagicMock(), TEST_PROJECT, FR):
            docs = [doc async for doc in group.document_iter]
            groups.append((group.language, docs))

    assert len(groups) == 1
    assert groups[0][0] == EN
    assert len(groups[0][1]) == 2


async def test_empty_source_yields_nothing() -> None:
    with patch(
        "translation_worker.activities._get_untranslated",
        side_effect=_empty_docs_iter,
    ):
        groups = [
            group
            async for group in _untranslated_by_language(MagicMock(), TEST_PROJECT, EN)
        ]

    assert groups == []


# _add_translation


async def test__add_translation__calls_async_bulk_once_per_invocation() -> None:
    bulk_call_count = 0

    async def capture_bulk(client, actions, **kwargs):
        nonlocal bulk_call_count
        list(actions)  # consume generator
        bulk_call_count += 1

    with patch("translation_worker.activities.async_bulk", side_effect=capture_bulk):
        await _add_translation(
            MagicMock(),
            MOCK_TRANSLATIONS,
            TEST_PROJECT,
            target_language_alpha_code=EN,
        )

    assert bulk_call_count == 1


async def test__add_translation__generates_update_action_per_translation() -> None:
    bulk_actions = []

    async def capture_bulk(client, actions, **kwargs):
        bulk_actions.extend(list(actions))

    with patch("translation_worker.activities.async_bulk", side_effect=capture_bulk):
        await _add_translation(
            MagicMock(),
            MOCK_TRANSLATIONS,
            TEST_PROJECT,
            target_language_alpha_code=EN,
        )

    assert len(bulk_actions) == 2
    assert all(a["_op_type"] == "update" for a in bulk_actions)
    assert all(a["_index"] == TEST_PROJECT for a in bulk_actions)


async def test__add_translation__sets_correct_doc_id_routing_and_params() -> None:
    bulk_actions = []

    async def capture_bulk(client, actions, **kwargs):
        bulk_actions.extend(list(actions))

    with patch("translation_worker.activities.async_bulk", side_effect=capture_bulk):
        await _add_translation(
            MagicMock(),
            MOCK_TRANSLATIONS[:1],
            TEST_PROJECT,
            target_language_alpha_code=EN,
        )

    assert bulk_actions[0][ID_] == DOC_ID_1
    assert bulk_actions[0]["_routing"] == ROOT_DOCUMENT_1

    params = bulk_actions[0]["script"]["params"]

    assert params["language"] == EN
    assert params["translation"] == MOCK_TRANSLATIONS[0][2]


# _get_untranslated


async def test__get_untranslated__yields_hits_from_a_single_page() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(**kwargs):
        yield {HITS: {HITS: [EN_DOC_1, EN_DOC_2]}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    results = await _collect_async(
        _get_untranslated(mock_es_client, TEST_PROJECT, target_language_alpha_code=EN)
    )

    assert results == [EN_DOC_1, EN_DOC_2]


async def test__get_untranslated__yields_hits_across_multiple_pages() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(**kwargs):
        yield {HITS: {HITS: [EN_DOC_1]}}
        yield {HITS: {HITS: [EN_DOC_2]}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    results = await _collect_async(
        _get_untranslated(mock_es_client, TEST_PROJECT, target_language_alpha_code=EN)
    )

    assert len(results) == 2


async def test__get_untranslated__empty_pages_yield_nothing() -> None:
    mock_es_client = MagicMock()

    mock_es_client.poll_search_pages = _empty_docs_iter

    results = await _collect_async(
        _get_untranslated(mock_es_client, TEST_PROJECT, target_language_alpha_code=EN)
    )

    assert results == []


# _count_untranslated


async def test__count_untranslated__returns_count_from_es_response() -> None:
    mock_es_client = AsyncMock()
    mock_es_client.count.return_value = {COUNT: 42}

    result = await _count_untranslated(
        mock_es_client, TEST_PROJECT, target_language_alpha_code=EN
    )

    assert result == 42
