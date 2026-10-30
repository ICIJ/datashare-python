import pytest

from extract_worker.objects import SimilarDocSearch


@pytest.mark.parametrize(("search", "expected_es_query"), [])
def test_search_similar_doc_search_to_es(
        search: SimilarDocSearch, expected_es_query: dict
) -> None:
    # When
    es_query = search.to_es()
    # Then
    assert es_query == expected_es_query
