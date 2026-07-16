# ruff: noqa: ARG001, ANN001, ANN202, FBT001, FBT002, ARG005

from collections.abc import AsyncGenerator, Iterable
from functools import partial
from typing import Any, Self, TypeVar
from unittest.mock import patch

import pytest
from datashare_python.objects import DatashareLanguage, Document, Language, Translation
from icij_common.es import (
    DOC_CONTENT,
    DOC_LANGUAGE,
    DOC_ROOT_ID,
    HITS,
    ID_,
    SOURCE,
    ESClient,
    ESSort,
)
from icij_common.registrable import RegistrableConfig
from translation_worker import activities
from translation_worker.activities import (
    _get_es_docs_by_language,
    _split_sentences,
    _update_docs_translation,
    create_translation_batches_act,
    translate_docs_act,
)
from translation_worker.config import (
    ArgosTranslatorConfig,
    TranslationModel,
    TranslationWorkerConfig,
)
from translation_worker.constants import DOC_CONTENT_TEXT_LENGTH
from translation_worker.objects import untranslated_query
from translation_worker.processors import SentenceSplitter, Translator

from tests.conftest import (
    DOC_ID_1,
    DOC_ID_2,
    DS_ENGLISH,
    DS_FRENCH,
    DS_SPANISH,
    ENGLISH,
    FRENCH,
    ROOT_DOCUMENT_1,
    ROOT_DOCUMENT_2,
    SPANISH,
    TEST_PROJECT,
)


class MockESClient(ESClient):
    def __init__(self, docs: list[dict[str, Any]]):
        super().__init__(pagination=10)
        self._docs = docs

    async def poll_search_pages(
        self,
        body: dict,  # noqa: ARG002
        sort: ESSort = None,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> AsyncGenerator[dict[str, Any], None]:
        if sort is not None and any("language" in s for s in sort):
            docs = sorted(self._docs, key=lambda x: x[SOURCE][DOC_LANGUAGE])
        else:
            docs = self._docs
        for i in range(0, len(docs), self._pagination_size):
            yield {HITS: {HITS: docs[i : i + self._pagination_size]}}


class MockSentenceSplitter(SentenceSplitter):
    def __init__(self, splits: list[list[str]]):
        self._splits = iter(splits)

    def load(self, language: Language) -> Self: ...

    def split_sentences(self, text: str) -> list[str]:  # noqa: ARG002
        return next(self._splits)

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...


class MockTranslator(Translator):
    registered_name = TranslationModel.ARGOS

    def __init__(self, translations: list[str]):
        super().__init__(ArgosTranslatorConfig())
        self._translations = translations

    def translate(self, texts: Iterable[str]) -> list[str]:  # noqa: ARG002
        return self._translations

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **extras) -> Self: ...


def _make_es_doc(
    doc_id: str, *, content: str, language: str, root_document: str
) -> dict:
    sources = {DOC_CONTENT: content, DOC_LANGUAGE: language, DOC_ROOT_ID: root_document}
    return {ID_: doc_id, SOURCE: sources}


async def _collect_async(gen: AsyncGenerator) -> list:
    return [item async for item in gen]


T = TypeVar("T")


async def _aiter(it: Iterable[T]) -> AsyncGenerator[T, None]:
    for item in it:
        yield item


EN_DOC_1_TEXT = "Hello"
EN_DOC_2_TEXT = "Goodbye"
FR_DOC_1_TEXT = "Bonjour"
FR_DOC_2_TEXT = "Au revoir"
ES_DOC_1_TEXT = "Hola"
ES_DOC_2_TEXT = "Adios"
EN_DOC_1 = _make_es_doc(
    DOC_ID_1, content=EN_DOC_1_TEXT, language=ENGLISH, root_document=ROOT_DOCUMENT_1
)
EN_DOC_2 = _make_es_doc(
    DOC_ID_2, content=EN_DOC_2_TEXT, language=ENGLISH, root_document=ROOT_DOCUMENT_2
)
FR_DOC_1 = _make_es_doc(
    DOC_ID_1, content=FR_DOC_1_TEXT, language=FRENCH, root_document=ROOT_DOCUMENT_1
)
FR_DOC_2 = _make_es_doc(
    DOC_ID_2, content=FR_DOC_2_TEXT, language=FRENCH, root_document=ROOT_DOCUMENT_2
)
ES_DOC_1 = _make_es_doc(
    DOC_ID_1, content=ES_DOC_1_TEXT, language=SPANISH, root_document=ROOT_DOCUMENT_1
)
ES_DOC_2 = _make_es_doc(
    DOC_ID_2, content=ES_DOC_2_TEXT, language=SPANISH, root_document=ROOT_DOCUMENT_2
)

# _iter_sentences

_EN_DOC_TO_SPLIT = _make_es_doc(
    DOC_ID_1,
    content="Hello world. How are you? I'm fine.",
    language=ENGLISH,
    root_document=ROOT_DOCUMENT_1,
)


@pytest.mark.parametrize(
    ("docs", "sentences", "expected_doc_sents"),
    [
        # sentences_with_correct_indices
        (
            [_EN_DOC_TO_SPLIT],
            [["Hello world.", "How are you?", "I'm fine."]],
            [
                (Document.from_es(_EN_DOC_TO_SPLIT), "Hello world."),
                (Document.from_es(_EN_DOC_TO_SPLIT), "How are you?"),
                (Document.from_es(_EN_DOC_TO_SPLIT), "I'm fine."),
            ],
        ),
        # multiple docs sentences
        (
            [EN_DOC_1, EN_DOC_2],
            [[EN_DOC_1_TEXT], [EN_DOC_2_TEXT]],
            [
                (Document.from_es(EN_DOC_1), EN_DOC_1_TEXT),
                (Document.from_es(EN_DOC_2), EN_DOC_2_TEXT),
            ],
        ),
        # no doc, no sentences
        ([], [], []),
    ],
)
async def test__iter_sentences__yields_sentences_with_correct_indices(
    docs: list[dict[str, Any]],
    sentences: list[list[str]],
    expected_doc_sents: list[tuple[Document, str]],
) -> None:
    # Given
    sentence_splitter = MockSentenceSplitter(sentences)
    docs = _aiter(docs)
    # When
    doc_sents = await _collect_async(_split_sentences(docs, sentence_splitter))
    # Then
    assert doc_sents == expected_doc_sents


# create_translation_batches
def _make_batching_doc(
    doc_id: str, language: DatashareLanguage, content_text_length: int = 0
) -> dict:
    source = {DOC_LANGUAGE: language, DOC_CONTENT_TEXT_LENGTH: content_text_length}
    return {ID_: doc_id, SOURCE: source}


async def test__create_translation_batches__returns_empty_list_when_no_docs() -> None:
    # Given
    client = MockESClient([])
    query = untranslated_query(DS_ENGLISH)
    # When
    result = [
        b
        async for b in create_translation_batches_act(
            project=TEST_PROJECT, query=query, es_client=client
        )
    ]
    # Then
    assert result == []


async def test__create_translation_batches__single_doc_creates_one_batch() -> None:
    # Given
    doc = _make_batching_doc(DOC_ID_1, FRENCH)
    client = MockESClient([doc])
    query = untranslated_query(DS_ENGLISH)
    # When
    result = [
        b
        async for b in create_translation_batches_act(
            project=TEST_PROJECT, query=query, es_client=client
        )
    ]
    # When
    assert len(result) == 1
    lang, batches = result[0]
    assert lang == DS_FRENCH
    assert batches == [[DOC_ID_1]]


async def test__create_translation_batches__multiple_docs_same_lang_one_batch() -> None:
    # Given
    query = untranslated_query(DS_ENGLISH)
    docs = [_make_batching_doc(DOC_ID_1, FRENCH), _make_batching_doc(DOC_ID_2, FRENCH)]
    client = MockESClient(docs)
    # When
    result = [
        b
        async for b in create_translation_batches_act(
            project=TEST_PROJECT, query=query, es_client=client
        )
    ]
    # Then
    assert len(result) == 1
    _, batches = result[0]
    assert batches == [[DOC_ID_1, DOC_ID_2]]


async def test__create_translation_batches__multiple_langs_yield_separate_entries() -> (
    None
):
    # Given
    query = untranslated_query(DS_ENGLISH)
    fr_doc = _make_batching_doc(DOC_ID_1, DS_FRENCH)
    es_doc = _make_batching_doc(DOC_ID_2, DS_SPANISH)
    docs = [fr_doc, es_doc]
    # When
    client = MockESClient(docs)
    result = [
        b
        async for b in create_translation_batches_act(
            project=TEST_PROJECT, query=query, es_client=client
        )
    ]
    # Then
    langs = {lang for lang, _ in result}
    assert len(result) == 2
    assert DS_FRENCH in langs
    assert DS_SPANISH in langs


async def test__create_translation_batches__splits_batch_if_max_text_len_exceeded() -> (
    None
):
    # Given
    batch_text_length = 1400
    doc_id_3 = "doc_id_3"
    query = untranslated_query(DS_ENGLISH)
    docs = [
        _make_batching_doc(DOC_ID_1, FRENCH, content_text_length=600),
        _make_batching_doc(DOC_ID_2, FRENCH, content_text_length=600),
        _make_batching_doc(doc_id_3, FRENCH, content_text_length=600),
    ]
    client = MockESClient(docs)
    # When
    result = [
        b
        async for b in create_translation_batches_act(
            project=TEST_PROJECT,
            query=query,
            batch_text_length=batch_text_length,
            es_client=client,
        )
    ]
    # Then
    _, batches = result[0]
    assert len(batches) == 2
    assert batches[0] == [DOC_ID_1, DOC_ID_2]
    assert batches[1] == [doc_id_3]


# translate_docs_act


async def test_translate_docs_act__returns_zero_for_empty_batch(monkeypatch) -> None:
    # Given
    batches = [[]]
    sentences = []
    translations = []
    translator = MockTranslator(translations)
    sentence_splitter = MockSentenceSplitter(sentences)
    es_client = MockESClient([FR_DOC_1, FR_DOC_2])
    # When
    n_translated = await translate_docs_act(
        batches,
        project=TEST_PROJECT,
        es_client=es_client,
        worker_config=TranslationWorkerConfig(),
        translator=translator,
        sentence_splitter=sentence_splitter,
    )
    # Then
    assert n_translated == 0


async def _do_nothing_es_update(
    es_client: ESClient,
    translated_docs: Iterable[tuple[Document, Translation]],
    project: str,
):
    pass


async def _capturing_es_update(
    es_client: ESClient,
    translated_docs: Iterable[tuple[Document, Translation]],
    project: str,
    captured: list[tuple[Document, Translation]],
):
    captured.extend(translated_docs)


async def test_translate_docs_act__returns_count_of_unique_docs_translated(
    monkeypatch,
) -> None:
    # Given
    batches = [[DOC_ID_1, DOC_ID_2]]
    sentences = [[FR_DOC_1_TEXT], [FR_DOC_2_TEXT]]
    translations = [EN_DOC_1_TEXT, EN_DOC_2_TEXT]
    translator = MockTranslator(translations)
    sentence_splitter = MockSentenceSplitter(sentences)
    monkeypatch.setattr(activities, "_update_docs_translation", _do_nothing_es_update)
    es_client = MockESClient([FR_DOC_1, FR_DOC_2])
    worker_config = TranslationWorkerConfig()
    # When
    with translator.load_cm(
        source=DS_ENGLISH, target=DS_ENGLISH, worker_config=worker_config
    ):
        n_translated = await translate_docs_act(
            batches,
            project=TEST_PROJECT,
            es_client=es_client,
            worker_config=worker_config,
            translator=translator,
            sentence_splitter=sentence_splitter,
        )
    # Then
    assert n_translated == 2


async def test_translate_docs_act__sentences_from_same_doc_count_as_one(
    monkeypatch,
) -> None:
    # Given
    batches = [[DOC_ID_1]]
    sentences = [[FR_DOC_1_TEXT, FR_DOC_2_TEXT]]
    translations = [EN_DOC_1_TEXT, EN_DOC_2_TEXT]
    translator = MockTranslator(translations)
    sentence_splitter = MockSentenceSplitter(sentences)
    monkeypatch.setattr(activities, "_update_docs_translation", _do_nothing_es_update)
    es_client = MockESClient([FR_DOC_1])
    worker_config = TranslationWorkerConfig()
    # When
    with translator.load_cm(
        source=DS_ENGLISH, target=DS_ENGLISH, worker_config=worker_config
    ):
        n_translated = await translate_docs_act(
            batches,
            project=TEST_PROJECT,
            es_client=es_client,
            worker_config=worker_config,
            translator=translator,
            sentence_splitter=sentence_splitter,
        )
    # Then
    assert n_translated == 1


async def test_translate_docs_act__es_update(monkeypatch) -> None:
    # Given
    batches = [[DOC_ID_1]]
    sentences = [[FR_DOC_1_TEXT, FR_DOC_2_TEXT]]
    translations = [EN_DOC_1_TEXT, EN_DOC_2_TEXT]
    translator = MockTranslator(translations)
    sentence_splitter = MockSentenceSplitter(sentences)
    captured = []
    update_doc = partial(_capturing_es_update, captured=captured)
    monkeypatch.setattr(activities, "_update_docs_translation", update_doc)
    es_client = MockESClient([FR_DOC_1])
    worker_config = TranslationWorkerConfig()
    # When
    with translator.load_cm(
        source=DS_FRENCH, target=DS_ENGLISH, worker_config=worker_config
    ):
        n_translated = await translate_docs_act(
            batches,
            project=TEST_PROJECT,
            es_client=es_client,
            worker_config=worker_config,
            translator=translator,
            sentence_splitter=sentence_splitter,
        )
    # Then
    assert n_translated == 1
    assert len(captured) == 1
    captured = captured[0]
    expected_translation = Translation(
        source_language=DS_FRENCH,
        target_language=DS_ENGLISH,
        translator=TranslationModel.ARGOS,
        content="Hello Goodbye",
    )
    expected = (Document.from_es(FR_DOC_1), expected_translation)
    assert captured == expected


async def test__update_docs() -> None:
    # Given
    doc_1 = Document(id="doc_1", language=DS_ENGLISH, root_document=ROOT_DOCUMENT_1)
    t_1 = Translation(
        source_language=DS_ENGLISH,
        target_language=DS_FRENCH,
        translator=TranslationModel.ARGOS,
        content="1",
    )
    doc_2 = Document(id="doc_2", language=DS_ENGLISH, root_document=ROOT_DOCUMENT_2)
    t_2 = Translation(
        source_language=DS_ENGLISH,
        target_language=DatashareLanguage(FRENCH),
        translator=TranslationModel.ARGOS,
        content="2",
    )
    translated_docs = [(doc_1, t_1), (doc_2, t_2)]

    # When
    with patch("translation_worker.activities.async_bulk") as mocked:
        await _update_docs_translation(MockESClient([]), translated_docs, TEST_PROJECT)

    # Then
    calls = mocked.mock_calls
    assert len(calls) == 1
    actions = list(calls[0].args[1])
    scripts = [a.pop("script") for a in actions]
    source_and_targets = [
        (
            s["params"]["translation"]["source_language"],
            s["params"]["translation"]["target_language"],
        )
        for s in scripts
    ]
    assert source_and_targets == [("ENGLISH", "FRENCH"), ("ENGLISH", "FRENCH")]
    expected_actions = [
        {
            "_id": "doc_1",
            "_index": "test-project",
            "_op_type": "update",
            "_routing": "root_document_1",
        },
        {
            "_id": "doc_2",
            "_index": "test-project",
            "_op_type": "update",
            "_routing": "root_document_2",
        },
    ]
    assert actions == expected_actions


# _get_es_docs


@pytest.mark.parametrize(
    ("docs", "expected_groups"),
    [
        # empty_docs_yields_no_doc
        ([], []),
        # single_doc
        ([FR_DOC_1], [[FR_DOC_1[ID_]]]),
        # groups_docs_of_same_language_together
        ([FR_DOC_1, FR_DOC_2], [[FR_DOC_1[ID_], FR_DOC_2[ID_]]]),
        # separate_group_per_language
        (
            [FR_DOC_1, ES_DOC_1, ES_DOC_2],
            [[FR_DOC_1[ID_]], [ES_DOC_1[ID_], ES_DOC_2[ID_]]],
        ),
    ],
)
async def test__get_es_docs(
    docs: list[dict[str, Any]], expected_groups: list[list[str]]
) -> None:
    # Given
    es_client = MockESClient(docs)
    # When
    docs = [
        [d async for d in group]
        async for group in _get_es_docs_by_language(
            es_client, TEST_PROJECT, DS_ENGLISH, []
        )
    ]
    docs = [[d[ID_] for d in g] for g in docs]
    # Then
    assert docs == expected_groups
