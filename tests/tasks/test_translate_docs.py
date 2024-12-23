# pylint: disable=redefined-outer-name
import logging
from typing import List

import pytest
from icij_common.es import ESClient

from datashare_python.objects import Document
from datashare_python.task_client import DatashareTaskClient
from datashare_python.tasks import create_translation_tasks
from datashare_python.tests.conftest import TEST_PROJECT

logger = logging.getLogger(__name__)


async def _progress(p: float):
    logger.info("progress: %s", p)


@pytest.mark.integration
async def test_create_translation_tasks_integration(
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_es_client: ESClient,
    test_task_client: DatashareTaskClient,
):
    # Given
    es_client = test_es_client
    task_client = test_task_client
    # When
    task_ids = await create_translation_tasks(
        project=TEST_PROJECT,
        target_language="ENGLISH",
        es_client=es_client,
        task_client=task_client,
    )
    # Then
    assert len(task_ids) == 2
