import os
import uuid

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import DatashareLanguage, Document
from icij_common.es import HITS, ESClient, has_type
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker
from translation_worker.constants import TaskQueue
from translation_worker.objects import (
    DefaultSentenceSplitterConfig,
    HunyuanMtTranslatorConfig,
    TranslationArgs,
    TranslationConfig,
    TranslationModel,
)
from translation_worker.workflows import TranslationWorkflow

from .conftest import CHINESE_TEXT, DS_ENGLISH


@pytest.mark.e2e
async def test_translation_workflow(  # noqa: PLR0917
    test_temporal_client: TemporalClient,  # noqa: ARG001
    index_translation_documents: list[Document],  # noqa: ARG001
    test_es_client: ESClient,
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    translation_inference_worker: Worker,  # noqa: ARG001
) -> None:
    # Given
    args = TranslationArgs(
        project=TEST_PROJECT,
        target_language=DatashareLanguage("ENGLISH"),
        config=TranslationConfig(),
    )
    workflow_id = f"translation-{uuid.uuid4().hex}"

    # When
    res = await test_temporal_client.execute_workflow(
        TranslationWorkflow.run, args, id=workflow_id, task_queue=TaskQueue.WORKFLOWS
    )

    assert res.n_translations == 2

    body = {"query": has_type(type_field="type", type_value="Document")}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]
    assert len(index_docs) == 2
    index_docs = [Document.from_es(doc) for doc in index_docs]
    assert all(
        any(ct.target_language == DS_ENGLISH for ct in doc.content_translated)
        for doc in index_docs
    )


_HUNYUAN_TEST_MODEL_REF = os.environ.get("TEST_HUNYUAN_MODEL_REF")


@pytest.mark.e2e
@pytest.mark.skipif(
    _HUNYUAN_TEST_MODEL_REF is None, reason="TEST_HUNYUAN_MODEL_REF is not set"
)
async def test_translation_workflow_with_hunyuan(  # noqa: PLR0917
    test_temporal_client_session: TemporalClient,
    index_chinese_translation_documents: list[Document],  # noqa: ARG001
    test_es_client: ESClient,
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    torch_translation_inference_worker: Worker,  # noqa: ARG001
) -> None:
    # Given
    args = TranslationArgs(
        project=TEST_PROJECT,
        target_language=DatashareLanguage("ENGLISH"),
        config=TranslationConfig(
            translator=HunyuanMtTranslatorConfig(
                model_ref=_HUNYUAN_TEST_MODEL_REF, max_new_tokens=256
            ),
            sentence_splitter=DefaultSentenceSplitterConfig(),
        ),
    )
    workflow_id = f"translation-{uuid.uuid4().hex}"

    # When
    res = await test_temporal_client_session.execute_workflow(
        TranslationWorkflow.run, args, id=workflow_id, task_queue=TaskQueue.WORKFLOWS
    )

    # Then
    assert res.n_translations == 1

    body = {"query": has_type(type_field="type", type_value="Document")}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]
    assert len(index_docs) == 1
    doc = Document.from_es(index_docs[0])
    translations = [
        ct
        for ct in doc.content_translated
        if ct.target_language == DS_ENGLISH
        and ct.translator == TranslationModel.HUNYUAN
    ]
    assert len(translations) == 1
    translated = translations[0].content
    assert "Beijing" in translated
    assert "翻译" not in translated
    assert CHINESE_TEXT[:5] not in translated
