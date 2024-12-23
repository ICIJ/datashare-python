import logging
from copy import copy, deepcopy
from functools import partial
from typing import List

import pytest
from icij_common.es import ESClient, HITS, has_type
from icij_common.test_utils import async_true_after
from icij_worker import TaskState
from icij_worker.ds_task_client import DatashareTaskClient
from numpy.random.mtrand import Sequence

from datashare_python.app import PYTHON_TASK_GROUP
from datashare_python.objects import Document
from datashare_python.tests.conftest import TEST_PROJECT

logger = logging.getLogger(__name__)


async def _progress(p: float):
    logger.info("progress: %s", p)


async def has_state(
    task_client: DatashareTaskClient,
    task_id: str,
    expected_state: TaskState | Sequence[TaskState],
    fail_early: TaskState | Sequence[TaskState] | None = None,
) -> bool:
    if isinstance(expected_state, TaskState):
        expected_state = (expected_state,)
    if isinstance(fail_early, TaskState):
        fail_early = (fail_early,)
    expected_state = set(expected_state)
    if fail_early:
        fail_early = set(fail_early)
    state = await task_client.get_task_state(task_id)
    if fail_early and state in fail_early:
        raise ValueError(f"Found invalid state {state}, expected {expected_state}")
    return state in expected_state


async def all_done(task_client: DatashareTaskClient, not_done: list[str]) -> bool:
    while not_done:
        for t_id in not_done:
            is_done = await has_state(
                task_client, t_id, TaskState.DONE, fail_early=TaskState.ERROR
            )
            if not is_done:
                return False
            not_done.remove(t_id)
    return True


@pytest.mark.integration
async def test_ping(
    test_task_client: DatashareTaskClient,
    worker_pool,  # pylint: disable=unused-argument
    app_lifetime_deps,  # pylint: disable=unused-argument
):
    # Given
    task_group = PYTHON_TASK_GROUP.name
    # When
    ping_task_id = await test_task_client.create_task("ping", dict(), group=task_group)
    # Then
    ping_timeout_s = 5.0
    assert await async_true_after(
        partial(
            has_state,
            test_task_client,
            ping_task_id,
            TaskState.DONE,
            fail_early=TaskState.ERROR,
        ),
        after_s=ping_timeout_s,
    )
    # When
    ping_result = await test_task_client.get_task_result(ping_task_id)
    assert ping_result == "pong"


@pytest.mark.integration
async def test_translate_and_classify(
    worker_pool,  # pylint: disable=unused-argument
    app_lifetime_deps,  # pylint: disable=unused-argument
    populate_es: List[Document],  # pylint: disable=unused-argument
    test_task_client: DatashareTaskClient,
    test_es_client: ESClient,
):
    # Given
    english = "ENGLISH"
    project = TEST_PROJECT
    task_group = PYTHON_TASK_GROUP.name
    n_workers = 2
    translation_args = {"target_language": english, "project": project}
    # When
    create_translation_tasks_id = await test_task_client.create_task(
        "create_translation_tasks", translation_args, group=task_group
    )

    # Then
    create_translation_timeout_s = 30
    assert await async_true_after(
        partial(
            has_state,
            test_task_client,
            create_translation_tasks_id,
            TaskState.DONE,
            fail_early=TaskState.ERROR,
        ),
        after_s=create_translation_timeout_s,
    )

    # Given
    translation_task_ids = await test_task_client.get_task_result(
        create_translation_tasks_id
    )
    # Then
    translation_timeout_s = 30
    assert await async_true_after(
        partial(all_done, test_task_client, deepcopy(translation_task_ids)),
        after_s=translation_timeout_s,
    )

    n_translated = 0
    for t in translation_task_ids:
        n_translated += await test_task_client.get_task_result(t)
    assert n_translated == 2

    # Given
    classification_args = {
        "language": english,
        "project": project,
        "n_workers": n_workers,
    }
    # When
    create_classifications_tasks_id = await test_task_client.create_task(
        "create_classification_tasks", classification_args, group=task_group
    )

    # Then
    create_classification_timeout_s = 10
    assert await async_true_after(
        partial(
            has_state,
            test_task_client,
            create_classifications_tasks_id,
            TaskState.DONE,
            fail_early=TaskState.ERROR,
        ),
        after_s=create_classification_timeout_s,
    )

    # Given
    classification_task_ids = await test_task_client.get_task_result(
        create_classifications_tasks_id
    )

    # Then
    classification_timeout_s = 30
    assert await async_true_after(
        partial(all_done, test_task_client, copy(classification_task_ids)),
        after_s=classification_timeout_s,
    )
    n_classified = 0
    for t in classification_task_ids:
        n_classified += await test_task_client.get_task_result(t)
    assert n_classified == 4

    body = {"query": has_type(type_field="type", type_value="Document")}
    sort = "_doc:asc"
    index_docs = []
    async for hits in test_es_client.poll_search_pages(
        index=TEST_PROJECT, body=body, sort=sort
    ):
        index_docs += hits[HITS][HITS]

    # Then
    assert len(index_docs) == 4
    index_docs = [Document.from_es(doc) for doc in index_docs]
    assert all(doc.tags for doc in index_docs)
