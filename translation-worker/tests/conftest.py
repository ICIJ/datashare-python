import asyncio
import uuid
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor

import pytest
from datashare_python.conftest import (  # noqa: F401
    TEST_PROJECT,
    event_loop,
    index_docs,
    test_deps,
    test_es_client,
    test_es_client_session,
    test_task_client,
    test_task_client_session,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from datashare_python.objects import Document
from datashare_python.types_ import TemporalClient
from icij_common.es import ESClient
from temporalio.worker import Worker
from translation_worker.activities import (
    CreateTranslationBatches,
    TranslateDocs,
    resolve_language_alpha_code,
)
from translation_worker.objects import TaskQueues, TranslationConfig
from translation_worker.workflows import TranslationWorkflow

EN = "en"
FR = "fr"
ES = "es"
ENGLISH = "english"
FRENCH = "french"
SPANISH = "spanish"
DOC_ID_1 = "doc_id_1"
DOC_ID_2 = "doc_id_2"
ROOT_DOCUMENT_1 = "root_document_1"
ROOT_DOCUMENT_2 = "root_document_2"

MOCK_TRANSLATIONS = [
    (DOC_ID_1, ROOT_DOCUMENT_1, "1"),
    (DOC_ID_2, ROOT_DOCUMENT_2, "2"),
]

FRENCH_TEXT = (
    "Dans le port d'Amsterdam, il y a des marins qui chantent les rêves "
    "y a des qui les hantent au large d'Amsterdam. Dans le port "
    "d'Amsterdam, il marins qui dorment comme des oriflammes le long "
    "des berges mornes."
)
SPANISH_TEXT = (
    "Besame.... Besame mucho.... Como si fuera esta noche la última "
    "vez. Besame.... Besame mucho.... Que tengo miedo perderte, "
    "perderte otra vez."
)


def _create_doc(
    doc_id: str, root_doc: str, text: str, language: str = "ENGLISH"
) -> Document:
    return Document(id=doc_id, root_document=root_doc, language=language, content=text)


@pytest.fixture
async def index_translation_documents(
    test_es_client: ESClient,  # noqa: F811
) -> list[Document]:
    docs = []
    languages = ["FRENCH", "SPANISH"]
    for idx, text in enumerate([FRENCH_TEXT, SPANISH_TEXT]):
        doc_id = f"doc_id_{idx}"
        root_document = f"root_document_{idx}"
        docs.append(_create_doc(doc_id, root_document, text, languages[idx]))
    async for _ in index_docs(test_es_client, docs=docs, index_name=TEST_PROJECT):
        pass
    return docs


@pytest.fixture(scope="session")
def translation_config() -> TranslationConfig:
    return TranslationConfig()


@pytest.fixture(scope="session")
async def batching_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    batching_worker_id = f"test-translation-batching-worker-{uuid.uuid4()}"
    create_translation_batches = CreateTranslationBatches(
        es_client=es_client,
        temporal_client=temporal_client,
        event_loop=event_loop,
    )
    batching_activities = [
        resolve_language_alpha_code,
        create_translation_batches.create_translation_batches,
    ]
    workflows = [TranslationWorkflow]
    batching_worker = Worker(
        temporal_client,
        identity=batching_worker_id,
        task_queue=TaskQueues.CPU,
        activities=batching_activities,
        workflows=workflows,
    )
    async with batching_worker:
        t = None
        try:
            t = asyncio.create_task(batching_worker.run())
            yield
        except Exception as e:  # noqa: BLE001
            if t is not None:
                t.cancel()
            raise e


@pytest.fixture(scope="session")
async def translation_worker(
    test_es_client_session: ESClient,  # noqa: F811
    test_temporal_client_session: TemporalClient,  # noqa: F811
    event_loop: asyncio.AbstractEventLoop,  # noqa: F811
) -> AsyncGenerator[None, None]:
    es_client = test_es_client_session
    temporal_client = test_temporal_client_session
    translation_worker_id = f"test-translation-translate-worker-{uuid.uuid4()}"
    translation_activities = [
        TranslateDocs(
            es_client=es_client,
            temporal_client=temporal_client,
            event_loop=event_loop,
        ).translate_docs,
    ]
    with ThreadPoolExecutor() as executor:
        translation_worker = Worker(
            temporal_client,
            identity=translation_worker_id,
            task_queue=TaskQueues.GPU,
            activities=translation_activities,
            activity_executor=executor,
        )
        async with translation_worker:
            t = None
            try:
                t = asyncio.create_task(translation_worker.run())
                yield
            except Exception as e:  # noqa: BLE001
                if t is not None:
                    t.cancel()
                raise e
