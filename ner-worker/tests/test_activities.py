from asyncio import AbstractEventLoop

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.types_ import TemporalClient
from icij_common.es import HITS, SOURCE, ESClient, has_type
from ner_worker.activities import NERActivity
from ner_worker.objects_ import NER, BatchDocument, Category, NamedEntity
from pydantic import TypeAdapter
from temporalio.testing import ActivityEnvironment


@pytest.mark.xfail(reason="skipping due to a bug in temoral test env")
async def test_spacy_ner_activity(
    doc_0: BatchDocument,
    doc_1: BatchDocument,
    doc_2: BatchDocument,
    doc_3: BatchDocument,
    test_temporal_client_session: TemporalClient,
    test_es_client: ESClient,
    event_loop: AbstractEventLoop,
    activity_env: ActivityEnvironment,
) -> None:
    # Given
    docs = [doc_0, doc_1, doc_2, doc_3]
    activity = NERActivity(test_temporal_client_session, event_loop)

    # When
    n_docs = await activity_env.run(activity.spacy_ner, docs)

    body = {"query": has_type(type_field="type", type_value="NamedEntity")}
    sort = ["offsets:asc"]
    index_ents = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_ents += [e[SOURCE] for e in hits[HITS][HITS]]

    # Then
    assert n_docs == 2
    index_ents = TypeAdapter(list[NamedEntity]).validate_python(index_ents)
    expected_entities = [
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
    ]
    assert index_ents == expected_entities
