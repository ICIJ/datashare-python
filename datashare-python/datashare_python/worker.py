import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import PollerBehaviorSimpleMaximum, Worker

from .discovery import Activity
from .types_ import TemporalClient

logger = logging.getLogger(__name__)

_SEPARATE_IO_AND_CPU_WORKERS = """The worker will run sync (CPU-bound) activities as \
well as IO-bound workflows.
To avoid deadlocks due to the GIL, we advise to run all CPU-bound activities inside a \
worker and workflows + IO-bound activities in a separate worker. Please have a look \
at temporal's documentation for more details:
 https://docs.temporal.io/develop/python/python-sdk-sync-vs-async#the-python-asynchronous-event-loop-and-blocking-calls
"""

_SEPARATE_IO_AND_CPU_ACTIVITIES = """The worker will run sync (CPU-bound) activities \
as well as IO-bound activities (async def).

To avoid deadlocks due to the GIL, we advise to run all CPU-bound activities inside a \
worker and IO-bound activities in a separate worker. Please have a look at temporal's \
documentation for more details:
 https://docs.temporal.io/develop/python/python-sdk-sync-vs-async#the-python-asynchronous-event-loop-and-blocking-calls
"""

_ACTIVITY_THREAD_NAME_PREFIX = "datashare-activity-worker-"


def datashare_worker(
    client: TemporalClient,
    *,
    workflows: list[type] | None = None,
    activities: list[Activity] | None = None,
    task_queue: str,
    # Scale horizontally be default for activities, each worker processes one activity
    # at a time
    max_concurrent_activities: int = 1,
) -> Worker:
    if workflows is None:
        workflows = []
    if activities is None:
        activities = []
    are_async = [a.__temporal_activity_definition.is_async for a in activities]
    if all(not a for a in are_async):
        activity_executor = ThreadPoolExecutor(
            thread_name_prefix=_ACTIVITY_THREAD_NAME_PREFIX
        )
    elif all(are_async):
        activity_executor = None  # Use temporal default
    else:
        activity_executor = ThreadPoolExecutor(
            thread_name_prefix=_ACTIVITY_THREAD_NAME_PREFIX
        )
        logger.warning(_SEPARATE_IO_AND_CPU_ACTIVITIES)

    if isinstance(activity_executor, ThreadPoolExecutor) and workflows:
        logger.warning(_SEPARATE_IO_AND_CPU_WORKERS)

    return Worker(
        client,
        workflows=workflows,
        activities=activities,
        task_queue=task_queue,
        activity_executor=activity_executor,
        max_concurrent_activities=max_concurrent_activities,
        # Let's make sure we poll activities one at a time otherwise, this will reserve
        # activities and prevent other workers to poll and process them
        activity_task_poller_behavior=PollerBehaviorSimpleMaximum(1),
        # Workflow tasks are assumed to be very lightweight and fast we can reserve
        # several of them
        workflow_task_poller_behavior=PollerBehaviorSimpleMaximum(5),
    )
