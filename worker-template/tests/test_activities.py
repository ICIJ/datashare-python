import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document
from icij_common.es import ESClient
from worker_template.activities import classify_docs, translate_docs


@pytest.mark.integration
async def test_translate_docs(
    populate_es: list[Document], test_es_client: ESClient
) -> None:
    # Given
    en_doc_0, en_doc_1, fr_doc, sp_doc = populate_es
    docs = [fr_doc.id, sp_doc.id]
    target_language = "ENGLISH"

    # When
    n_translated = await translate_docs(
        docs, target_language, project=TEST_PROJECT, es_client=test_es_client
    )

    assert n_translated == 2


@pytest.mark.integration
async def test_classify_docs(
    populate_es: list[Document], test_es_client: ESClient
) -> None:
    # Given
    en_doc_0, en_doc_1, fr_doc, sp_doc = populate_es
    docs = [en_doc_0.id, en_doc_1.id]
    classified_language = "ENGLISH"

    # When
    n_translated = await classify_docs(
        docs,
        classified_language=classified_language,
        project=TEST_PROJECT,
        es_client=test_es_client,
    )

    assert n_translated == 2
