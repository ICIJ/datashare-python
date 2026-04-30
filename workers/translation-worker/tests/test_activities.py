# ruff: noqa: ARG001, ANN001, ANN202, FBT001, FBT002, ARG005

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from icij_common.es import (
    BOOL,
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
    _get_doc_contents_and_split_on_sentences,
    _get_es_docs,
    _iter_sentences,
    _translate_batch,
    _untranslated_query,
    create_translation_batches,
    translate_docs,
)
from translation_worker.constants import CONTENT_LENGTH
from translation_worker.core import get_translation_ensemble
from translation_worker.objects import (
    BatchSentence,
    TranslationWorkerConfig,
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
ES_DOC_1_TEXT = "Hola"
ES_DOC_2_TEXT = "Adios"
EN_DOC_1 = _make_es_doc(DOC_ID_1, EN_DOC_1_TEXT, ENGLISH, ROOT_DOCUMENT_1)
EN_DOC_2 = _make_es_doc(DOC_ID_2, EN_DOC_2_TEXT, ENGLISH, ROOT_DOCUMENT_2)
FR_DOC_1 = _make_es_doc(DOC_ID_1, FR_DOC_1_TEXT, FRENCH, ROOT_DOCUMENT_1)
FR_DOC_2 = _make_es_doc(DOC_ID_2, FR_DOC_2_TEXT, FRENCH, ROOT_DOCUMENT_2)
ES_DOC_1 = _make_es_doc(DOC_ID_1, ES_DOC_1_TEXT, SPANISH, ROOT_DOCUMENT_1)
ES_DOC_2 = _make_es_doc(DOC_ID_2, ES_DOC_2_TEXT, SPANISH, ROOT_DOCUMENT_2)


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


async def test__iter_sentences__yields_sentences_with_correct_indices() -> None:
    es_doc = _make_es_doc(
        DOC_ID_1, "Hello world. How are you? I'm fine.", EN, ROOT_DOCUMENT_1
    )

    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [
        "Hello world.",
        "How are you?",
        "I'm fine.",
    ]
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    batches = await _collect_async(
        _iter_sentences(
            _single_doc_iter(es_doc),
            translation_ensemble,
        )
    )

    non_empty = [b for b in batches if b]
    assert len(non_empty) == 1
    assert len(non_empty[0]) == 3
    assert [s.sentence_index for s in non_empty[0]] == [0, 1, 2]


async def test__iter_sentences__doc_id_and_root_document_are_preserved() -> None:
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [FR_DOC_1_TEXT]
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    batches = await _collect_async(
        _iter_sentences(
            _single_doc_iter(FR_DOC_1),
            translation_ensemble,
        )
    )

    non_empty = [b for b in batches if b]
    sentence = non_empty[0][0]
    assert sentence.doc_id == DOC_ID_1
    assert sentence.root_document == ROOT_DOCUMENT_1


async def test__iter_sentences__multiple_docs_sentences_collected_into_one_batch() -> (
    None
):
    async def docs_iter():
        yield EN_DOC_1
        yield EN_DOC_2

    sentencizer = MagicMock()
    sentencizer.split_sentences.side_effect = [[EN_DOC_1_TEXT], [EN_DOC_2_TEXT]]
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    batches = await _collect_async(_iter_sentences(docs_iter(), translation_ensemble))

    non_empty = [b for b in batches if b]
    assert len(non_empty) == 1
    assert len(non_empty[0]) == 2


async def test__iter_sentences__empty_docs_iter_yields_no_sentences() -> None:
    batches = await _collect_async(_iter_sentences(_empty_docs_iter(), MagicMock()))

    assert not any(batches)


# create_translation_batches


def _make_batching_doc(doc_id: str, language: str, content_length: int = 0) -> dict:
    return {
        ID_: doc_id,
        SOURCE: {DOC_LANGUAGE: language, CONTENT_LENGTH: content_length},
    }


async def _make_group(*docs):
    for doc in docs:
        yield doc


async def test__create_translation_batches__returns_empty_list_when_no_docs() -> None:
    async def mock_get_es_docs(*args, **kwargs):
        return
        yield

    with patch(
        "translation_worker.activities._get_es_docs", side_effect=mock_get_es_docs
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language=EN,
        )

    assert result == []


async def test__create_translation_batches__single_doc_creates_one_batch() -> None:
    doc = _make_batching_doc(DOC_ID_1, FRENCH)

    async def mock_get_es_docs(*args, **kwargs):
        yield _make_group(doc)

    with patch(
        "translation_worker.activities._get_es_docs", side_effect=mock_get_es_docs
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language=EN,
        )

    assert len(result) == 1
    lang, batches = result[0]
    assert lang == FR
    assert batches == [[DOC_ID_1]]


async def test__create_translation_batches__multiple_docs_same_lang_one_batch() -> None:
    docs = [_make_batching_doc(DOC_ID_1, FRENCH), _make_batching_doc(DOC_ID_2, FRENCH)]

    async def mock_get_es_docs(*args, **kwargs):
        yield _make_group(*docs)

    with patch(
        "translation_worker.activities._get_es_docs", side_effect=mock_get_es_docs
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language=EN,
        )

    assert len(result) == 1
    _, batches = result[0]
    assert batches == [[DOC_ID_1, DOC_ID_2]]


async def test__create_translation_batches__multiple_langs_yield_separate_entries() -> (
    None
):
    fr_doc = _make_batching_doc(DOC_ID_1, FRENCH)
    es_doc = _make_batching_doc(DOC_ID_2, SPANISH)

    async def mock_get_es_docs(*args, **kwargs):
        yield _make_group(fr_doc)
        yield _make_group(es_doc)

    with patch(
        "translation_worker.activities._get_es_docs", side_effect=mock_get_es_docs
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT,
            target_language=EN,
        )

    langs = [lang for lang, _ in result]
    assert len(result) == 2
    assert FR in langs
    assert ES in langs


async def test__create_translation_batches__splits_batch_if_max_byte_len_exceeded() -> (
    None
):
    # doc1 is the first doc (byte len not yet tracked); doc2 adds 600 bytes;
    # doc3 would bring total to 1200 > 1000, so it triggers a flush before being added
    doc_id_3 = "doc_id_3"
    docs = [
        _make_batching_doc(DOC_ID_1, FRENCH, content_length=600),
        _make_batching_doc(DOC_ID_2, FRENCH, content_length=600),
        _make_batching_doc(doc_id_3, FRENCH, content_length=600),
    ]

    async def mock_get_es_docs(*args, **kwargs):
        yield _make_group(*docs)

    with patch(
        "translation_worker.activities._get_es_docs", side_effect=mock_get_es_docs
    ):
        result = await create_translation_batches(
            project=TEST_PROJECT, target_language=EN, max_batch_byte_len=1000
        )

    _, batches = result[0]
    assert len(batches) == 2
    assert batches[0] == [DOC_ID_1, DOC_ID_2]
    assert batches[1] == [doc_id_3]


# translate_docs


async def test_translate_docs__returns_zero_for_empty_batch() -> None:
    empty_batch = (FR, [])
    result = await translate_docs(
        empty_batch,
        EN,
        project=TEST_PROJECT,
        es_client=MagicMock(),
        worker_config=TranslationWorkerConfig(),
    )
    assert result == 0


async def test_translate_docs__accepts_none_config_and_defaults() -> None:
    empty_batch = (FR, [])
    result = await translate_docs(
        empty_batch,
        EN,
        project=TEST_PROJECT,
        es_client=MagicMock(),
        worker_config=None,
    )
    assert result == 0


async def test_translate_docs__returns_count_of_unique_docs_translated() -> None:
    sentences = [
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=0,
            sentence=FR_DOC_1_TEXT,
        ),
        BatchSentence(
            doc_id=DOC_ID_2,
            root_document=ROOT_DOCUMENT_2,
            sentence_index=0,
            sentence=FR_DOC_2_TEXT,
        ),
    ]

    async def mock_split(*args, **kwargs):
        yield sentences

    async def mock_translate(batch, ensemble, config):
        return [EN_DOC_1_TEXT, EN_DOC_2_TEXT]

    with (
        patch(
            "translation_worker.activities.get_translation_ensemble",
            return_value=MagicMock(),
        ),
        patch(
            "translation_worker.activities._get_doc_contents_and_split_on_sentences",
            side_effect=mock_split,
        ),
        patch(
            "translation_worker.activities._translate_batch", side_effect=mock_translate
        ),
        patch("translation_worker.activities._add_translation", new_callable=AsyncMock),
    ):
        result = await translate_docs(
            (FR, [[DOC_ID_1, DOC_ID_2]]),
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            worker_config=TranslationWorkerConfig(),
        )

    assert result == 2


async def test_translate_docs__sentences_from_same_doc_count_as_one() -> None:
    sentences = [
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=0,
            sentence=FR_DOC_1_TEXT,
        ),
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=1,
            sentence=FR_DOC_2_TEXT,
        ),
    ]

    async def mock_split(*args, **kwargs):
        yield sentences

    async def mock_translate(batch, ensemble, config):
        return [EN_DOC_1_TEXT, EN_DOC_2_TEXT]

    with (
        patch(
            "translation_worker.activities.get_translation_ensemble",
            return_value=MagicMock(),
        ),
        patch(
            "translation_worker.activities._get_doc_contents_and_split_on_sentences",
            side_effect=mock_split,
        ),
        patch(
            "translation_worker.activities._translate_batch", side_effect=mock_translate
        ),
        patch("translation_worker.activities._add_translation", new_callable=AsyncMock),
    ):
        result = await translate_docs(
            (FR, [[DOC_ID_1]]),
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            worker_config=TranslationWorkerConfig(),
        )

    assert result == 1


async def test_translate_docs__reconstructs_translation_in_sentence_order() -> None:
    sentences = [
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=0,
            sentence=FR_DOC_1_TEXT,
        ),
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=1,
            sentence=FR_DOC_2_TEXT,
        ),
    ]
    captured_translations = []

    async def mock_split(*args, **kwargs):
        yield sentences

    async def mock_translate(batch, ensemble, config):
        return [EN_DOC_1_TEXT, EN_DOC_2_TEXT]

    async def mock_add_translation(
        es_client, translations, project, *, target_language_alpha_code
    ):
        captured_translations.extend(translations)

    with (
        patch(
            "translation_worker.activities.get_translation_ensemble",
            return_value=MagicMock(),
        ),
        patch(
            "translation_worker.activities._get_doc_contents_and_split_on_sentences",
            side_effect=mock_split,
        ),
        patch(
            "translation_worker.activities._translate_batch", side_effect=mock_translate
        ),
        patch(
            "translation_worker.activities._add_translation",
            side_effect=mock_add_translation,
        ),
    ):
        await translate_docs(
            (FR, [[DOC_ID_1]]),
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            worker_config=TranslationWorkerConfig(),
        )

    assert len(captured_translations) == 1
    _, _, combined = captured_translations[0]
    assert EN_DOC_1_TEXT in combined
    assert EN_DOC_2_TEXT in combined
    assert combined.index(EN_DOC_1_TEXT) < combined.index(EN_DOC_2_TEXT)


async def test_translate_docs__calls_add_translation_with_target_language() -> None:
    sentences = [
        BatchSentence(
            doc_id=DOC_ID_1,
            root_document=ROOT_DOCUMENT_1,
            sentence_index=0,
            sentence=FR_DOC_1_TEXT,
        ),
    ]
    captured_kwargs = {}

    async def mock_split(*args, **kwargs):
        yield sentences

    async def mock_add_translation(
        es_client, translations, project, *, target_language_alpha_code
    ):
        captured_kwargs["target_language_alpha_code"] = target_language_alpha_code

    with (
        patch(
            "translation_worker.activities.get_translation_ensemble",
            return_value=MagicMock(),
        ),
        patch(
            "translation_worker.activities._get_doc_contents_and_split_on_sentences",
            side_effect=mock_split,
        ),
        patch(
            "translation_worker.activities._translate_batch",
            side_effect=lambda b, e, c: [EN_DOC_1_TEXT],
        ),
        patch(
            "translation_worker.activities._add_translation",
            side_effect=mock_add_translation,
        ),
    ):
        await translate_docs(
            (FR, [[DOC_ID_1]]),
            EN,
            project=TEST_PROJECT,
            es_client=MagicMock(),
            worker_config=TranslationWorkerConfig(),
        )

    assert captured_kwargs["target_language_alpha_code"] == EN


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
    translation_ensemble = get_translation_ensemble(
        source_language_alpha_code=EN,
        target_language_alpha_code=FR,
    )
    with patch(
        "translation_worker.activities.translate_as_list",
        return_value=[FR_DOC_1_TEXT],
    ):
        result = await _translate_batch(sentences, translation_ensemble)

    assert result == [FR_DOC_1_TEXT]


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


# _get_es_docs


async def test__get_es_docs__empty_docs_yields_no_groups() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    groups = [
        group async for group in _get_es_docs(mock_es_client, TEST_PROJECT, EN, [])
    ]

    assert groups == []


async def test__get_es_docs__single_doc_yields_one_group() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    groups = []
    async for group in _get_es_docs(mock_es_client, TEST_PROJECT, EN, []):
        groups.append([doc async for doc in group])

    assert len(groups) == 1
    assert groups[0][0][ID_] == DOC_ID_1


async def test__get_es_docs__groups_docs_of_same_language_together() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1, FR_DOC_2]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    groups = []
    async for group in _get_es_docs(mock_es_client, TEST_PROJECT, EN, []):
        groups.append([doc async for doc in group])

    assert len(groups) == 1
    assert len(groups[0]) == 2


async def test__get_es_docs__yields_separate_group_per_language() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1, ES_DOC_1]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    groups = []
    async for group in _get_es_docs(mock_es_client, TEST_PROJECT, EN, []):
        groups.append([doc async for doc in group])

    assert len(groups) == 2
    assert groups[0][0][SOURCE][DOC_LANGUAGE] == FRENCH
    assert groups[1][0][SOURCE][DOC_LANGUAGE] == SPANISH


async def test__get_es_docs__all_docs_in_second_language_group_are_included() -> None:
    mock_es_client = MagicMock()

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1, ES_DOC_1, ES_DOC_2]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    groups = []
    async for group in _get_es_docs(mock_es_client, TEST_PROJECT, EN, []):
        groups.append([doc async for doc in group])

    assert len(groups) == 2
    assert len(groups[1]) == 2


# _get_doc_contents_and_split_on_sentences


async def test__get_doc_contents_and_split_on_sentences__empty_iter_yields_empty() -> (
    None
):
    result = await _collect_async(
        _get_doc_contents_and_split_on_sentences(
            MagicMock(), TEST_PROJECT, [], MagicMock()
        )
    )

    assert result == []


async def test__get_doc_contents_and_split_on_sentences__yields_sents_from_doc() -> (
    None
):
    mock_es_client = MagicMock()
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = [FR_DOC_1_TEXT]
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    batches = await _collect_async(
        _get_doc_contents_and_split_on_sentences(
            mock_es_client,
            TEST_PROJECT,
            [DOC_ID_1],
            translation_ensemble,
        )
    )

    assert len(batches) == 1
    assert batches[0][0].doc_id == DOC_ID_1
    assert batches[0][0].sentence == FR_DOC_1_TEXT


async def test__get_doc_contents_and_split_on_sentences__yields_last_batch() -> None:
    # batch_size is 16 but the doc has only 2 sentences — partial batch must
    # still be yielded
    mock_es_client = MagicMock()
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = ["Sentence one.", "Sentence two."]
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    batches = await _collect_async(
        _get_doc_contents_and_split_on_sentences(
            mock_es_client,
            TEST_PROJECT,
            [DOC_ID_1],
            translation_ensemble,
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 2


async def test__get_doc_contents_and_split_on_sentences__many_batches_batch_size() -> (
    None
):
    mock_es_client = MagicMock()
    sentences = ["One.", "Two.", "Three."]
    sentencizer = MagicMock()
    sentencizer.split_sentences.return_value = sentences
    translation_ensemble = MagicMock()
    translation_ensemble.sentencizer = sentencizer

    async def mock_poll_search_pages(*args, **kwargs):
        yield {HITS: {HITS: [FR_DOC_1]}}
        yield {HITS: {HITS: []}}

    mock_es_client.poll_search_pages = mock_poll_search_pages

    batches = await _collect_async(
        _get_doc_contents_and_split_on_sentences(
            mock_es_client,
            TEST_PROJECT,
            [DOC_ID_1],
            translation_ensemble,
            sentence_batch_size=2,
        )
    )

    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1
