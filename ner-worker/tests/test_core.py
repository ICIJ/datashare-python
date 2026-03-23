import pytest

from objects_ import Category, NER, NamedEntity


@pytest.mark.integration
@pytest.mark.parametrize(
    ("categories", "expected_entities"),
    [
        (
            None,
            [
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Dan",
                    category=Category.PER,
                    offsets=[57],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Paris",
                    category=Category.LOC,
                    offsets=[93, 103],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Intel",
                    category=Category.ORG,
                    offsets=[161],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
            ],
        ),
        (
            ["LOCATION", "PERSON", "ORGANIZATION"],
            [
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Dan",
                    category=Category.PER,
                    offsets=[57],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Paris",
                    category=Category.LOC,
                    offsets=[93, 103],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Intel",
                    category=Category.ORG,
                    offsets=[161],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
            ],
        ),
        (
            ["LOCATION"],
            [
                NamedEntity(
                    document_id="doc-0",
                    root_document="root-0",
                    mention="Paris",
                    category=Category.LOC,
                    offsets=[93, 103],
                    extractor_language="ENGLISH",
                    extractor=NER.SPACY,
                ),
            ],
        ),
    ],
)
def test_extract_and_write_doc_ner_tags() -> None:
    assert False
