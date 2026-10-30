import pytest
from datashare_python.objects import Document
from extract_worker.cli.search import _display_candidates
from icij_common.es import ID_, INDEX_, SOURCE


@pytest.fixture
def es_docs(indexed_docs: list[Document]) -> list[dict]:
    as_es = []
    for doc in indexed_docs:
        doc = {SOURCE: doc.model_dump(), ID_: doc.id, INDEX_: doc.index}
        as_es.append(doc)
    return as_es


def test_display_candidates(es_docs: list[dict]) -> None:
    # Given
    base_url = "http://my-datashare.org"
    # When
    _display_candidates(es_docs, base_url, page_size=5)
