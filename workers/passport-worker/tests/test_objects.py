import pytest
from passport_worker.objects import PassportProcessingError


@pytest.mark.regression
def test_should_deserialize_processing_error() -> None:
    # Given
    error = {
        "source": {
            "doc_id": "",
            "project": "",
            "n_pages": 1,
            "value": {"path": "", "location": "filesystem"},
            "parent": {
                "id": "",
                "language": "ENGLISH",
                "index": "diana",
                "root_document": "",
                "content": None,
                "extraction_level": 0,
                "path": "",
                "tags": [],
                "metadata": {},
                "type": "Document",
            },
        },
        "error": {"title": "InvalidPDF", "detail": ""},
    }
    # When
    deser = PassportProcessingError.model_validate(error)
    # Then
    assert isinstance(deser, PassportProcessingError)
