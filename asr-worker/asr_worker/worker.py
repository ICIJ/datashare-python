import asyncio
import logging
import os
import socket
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from temporalio import workflow

from asr_worker.constants import (
    ASR_TASK_QUEUE,
    ASR_WORKER_NAME,
    DEFAULT_TEMPORAL_ADDRESS,
)
from asr_worker.workflow import ASRWorkflow

with workflow.unsafe.imports_passed_through():
    from asr_worker.activities import ASRActivities

LOGGER = logging.getLogger(__name__)


async def run_asr_worker(target_host: str = DEFAULT_TEMPORAL_ADDRESS) -> None:
    """ASR worker task"""
    LOGGER.info("ASR worker connecting to target_host={%s}", target_host)
    client = await Client.connect(target_host=target_host, namespace="default")
    identity = f"{ASR_WORKER_NAME}:{os.getpid()}@{socket.gethostname()}"
    activities = ASRActivities()
    worker = Worker(
        client=client,
        task_queue=ASR_TASK_QUEUE,
        workflows=[ASRWorkflow],
        activities=[activities.preprocess, activities.infer, activities.postprocess],
        activity_executor=ThreadPoolExecutor(),
        identity=identity,
    )
    LOGGER.info("Activities loaded. ASR worker starting...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_asr_worker())
