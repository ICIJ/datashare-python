import pytest
from classify import classify_docs
from conftest import TEST_PROJECT
from icij_common.es import ESClient
from objects import Document


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
