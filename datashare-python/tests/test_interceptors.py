import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Annotated, Any

import pytest
import temporalio
from datashare_python.objects import DatashareModel
from datashare_python.utils import (
    ActivityWithProgress,
    WorkflowWithProgress,
    activity_defn,
    execute_activity,
)
from temporalio import exceptions as temporalio_exceptions
from temporalio.common import RetryPolicy

with temporalio.workflow.unsafe.imports_passed_through():
    from datashare_python.config import WorkerConfig
    from datashare_python.interceptors import (
        HeartbeatInterceptor,
        ProgressInterceptor,
        TraceContext,
        TraceContextInterceptor,
        get_trace_context,
    )
    from datashare_python.types_ import ProgressRateHandler, TemporalClient, Weight
    from datashare_python.utils import PYDANTIC_DATA_CONVERTER
    from temporalio import workflow
    from temporalio.client import (
        Interceptor,
        OutboundInterceptor,
        StartWorkflowInput,
        WorkflowFailureError,
        WorkflowHandle,
    )
    from temporalio.converter import DataConverter
    from temporalio.worker import Worker


logger = logging.getLogger(__name__)


class TestTaskQueue:
    HEARTBEAT = "test.heartbeat"
    NO_HEARTBEAT = "test.no_heartbeat"
    PROGRESS_SYNC = "test.progress.sync"
    PROGRESS_ASYNC = "test.progress.async"
    TRACE = "test.trace"
    WORKFLOWS = "test.workflows"


_DUMMY_TRACE_CTX = TraceContext(version="00", trace_id="trace_id", parent_id="trace_id")
_DEFAULT_PAYLOAD_CONVERTER = DataConverter.default.payload_converter


class _MockOutboundInterceptor(OutboundInterceptor):
    async def start_workflow(
        self,
        input: StartWorkflowInput,  # noqa: A002
    ) -> WorkflowHandle[Any, Any]:
        input.headers["traceparent"] = _DEFAULT_PAYLOAD_CONVERTER.to_payload(
            _DUMMY_TRACE_CTX.traceparent
        )
        return await self.next.start_workflow(input)


class _MockTraceContextHeaderInterceptor(Interceptor):
    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:  # noqa: A002
        return super().intercept_client(_MockOutboundInterceptor(next))


_TIMEOUT = timedelta(seconds=180)


@workflow.defn
class _TestTraceContentWorkflow:
    @workflow.run
    async def run(self) -> list[TraceContext]:
        current_ctx = get_trace_context()
        ctx_log = [current_ctx]
        ctx_log = await workflow.execute_activity(
            ctx_test_act,
            ctx_log,
            task_queue=TestTaskQueue.TRACE,
            start_to_close_timeout=_TIMEOUT,
        )
        ctx_log = await workflow.execute_activity(
            ctx_test_act,
            ctx_log,
            task_queue=TestTaskQueue.TRACE,
            start_to_close_timeout=_TIMEOUT,
        )
        return ctx_log


@activity_defn(name="ctx-test")
async def ctx_test_act(previous: list[TraceContext]) -> list[TraceContext]:
    previous.append(get_trace_context())
    return previous


@activity_defn(name="sleep-for-act")
async def sleep_for_act(duration: float) -> None:
    await asyncio.sleep(duration)


class ProgressArg(DatashareModel):
    name: str


class _ProgressAct(ActivityWithProgress):
    @activity_defn("hello-async")
    async def hello_async_act(
        self,
        args: ProgressArg,
        # We test variable args lengths doesn't break deserialization:
        # https://github.com/temporalio/sdk-python/issues/360
        extra: str | None = None,
        *,
        progress: Annotated[ProgressRateHandler | None, Weight(value=5.0)] = None,
    ) -> str:
        if progress is not None:
            await progress(0.1)
        hello = f"hello {args.name}"
        if extra:
            hello += f"{hello} + {extra}"
        if progress is not None:
            await progress(0.9)
        return hello

    @activity_defn("hello-sync")
    def hello_sync_act(
        self,
        args: ProgressArg,
        *,
        progress: ProgressRateHandler | None = None,
    ) -> str:
        if progress is not None:
            self._event_loop.run_until_complete(progress(0.1))
        hello = f"hello {args.name}"
        if progress is not None:
            self._event_loop.run_until_complete(progress(0.9))
        return hello


@workflow.defn(name="progress")
class _TestProgressWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self, args: ProgressArg) -> None:
        await execute_activity(
            _ProgressAct.hello_sync_act,
            args=[args],
            task_queue=TestTaskQueue.PROGRESS_SYNC,
            start_to_close_timeout=_TIMEOUT,
        )
        await execute_activity(
            _ProgressAct.hello_async_act,
            args=[args],
            task_queue=TestTaskQueue.PROGRESS_ASYNC,
            start_to_close_timeout=_TIMEOUT,
        )


@workflow.defn(name="heartbeat")
class _TestHeartbeatWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self) -> None:
        await execute_activity(
            sleep_for_act,
            arg=1,
            task_queue=TestTaskQueue.HEARTBEAT,
            start_to_close_timeout=_TIMEOUT,
            heartbeat_timeout=timedelta(milliseconds=100),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


@workflow.defn(name="no-heartbeat")
class _TestNoHeartbeatWorkflow(WorkflowWithProgress):
    @workflow.run
    async def run(self) -> None:
        await execute_activity(
            sleep_for_act,
            arg=1,
            task_queue=TestTaskQueue.NO_HEARTBEAT,
            start_to_close_timeout=_TIMEOUT,
            heartbeat_timeout=timedelta(milliseconds=100),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


@pytest.fixture(scope="session")
async def test_wf_worker(
    test_temporal_client_session: TemporalClient,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-interceptor-worker-{uuid.uuid4()}"
    interceptors = [TraceContextInterceptor()]
    wfs = [_TestTraceContentWorkflow, _TestProgressWorkflow]
    worker = Worker(
        client,
        identity=worker_id,
        workflows=wfs,
        interceptors=interceptors,
        task_queue=TestTaskQueue.WORKFLOWS,
    )
    async with worker:
        yield


@pytest.fixture(scope="session")
async def test_trace_worker(
    test_temporal_client_session: TemporalClient,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-interceptor-worker-{uuid.uuid4()}"
    activities = [ctx_test_act]
    interceptors = [TraceContextInterceptor()]
    worker = Worker(
        client,
        identity=worker_id,
        activities=activities,
        interceptors=interceptors,
        task_queue=TestTaskQueue.TRACE,
    )
    async with worker:
        yield


@pytest.fixture(scope="session")
async def test_progress_interceptor_sync_worker(
    test_temporal_client_session: TemporalClient, event_loop: asyncio.AbstractEventLoop
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-progress-sync-worker-{uuid.uuid4()}"
    interceptors = [ProgressInterceptor()]
    act = _ProgressAct(test_temporal_client_session, event_loop)
    worker = Worker(
        client,
        identity=worker_id,
        activities=[act.hello_sync_act],
        activity_executor=ThreadPoolExecutor(),
        interceptors=interceptors,
        task_queue=TestTaskQueue.PROGRESS_SYNC,
    )
    async with worker:
        yield


@pytest.fixture(scope="session")
async def test_progress_interceptor_async_worker(
    test_temporal_client_session: TemporalClient, event_loop: asyncio.AbstractEventLoop
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-progress-async-worker-{uuid.uuid4()}"
    interceptors = [ProgressInterceptor()]
    act = _ProgressAct(test_temporal_client_session, event_loop)
    worker = Worker(
        client,
        identity=worker_id,
        activities=[act.hello_async_act],
        interceptors=interceptors,
        task_queue=TestTaskQueue.PROGRESS_ASYNC,
    )
    async with worker:
        yield


@pytest.fixture(scope="session")
async def test_heartbeat_interceptor_worker(
    test_temporal_client_session: TemporalClient,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-heartbeat-worker-{uuid.uuid4()}"
    interceptors = [HeartbeatInterceptor()]
    worker = Worker(
        client,
        identity=worker_id,
        workflows=[_TestHeartbeatWorkflow],
        activities=[sleep_for_act],
        interceptors=interceptors,
        task_queue=TestTaskQueue.HEARTBEAT,
    )
    async with worker:
        yield


@pytest.fixture(scope="session")
async def test_heartbeat_interceptor_no_heartbeat_worker(
    test_temporal_client_session: TemporalClient,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-heartbeat-worker-{uuid.uuid4()}"
    worker = Worker(
        client,
        identity=worker_id,
        workflows=[_TestNoHeartbeatWorkflow],
        activities=[sleep_for_act],
        task_queue=TestTaskQueue.NO_HEARTBEAT,
    )
    async with worker:
        yield


async def test_trace_context_interceptor(
    test_wf_worker,  # noqa: ANN001, ARG001
    test_trace_worker,  # noqa: ANN001, ARG001
    test_worker_config: WorkerConfig,
) -> None:
    # Given
    temporal_config = test_worker_config.temporal
    client = await TemporalClient.connect(
        target_host=temporal_config.host,
        namespace=temporal_config.namespace,
        data_converter=PYDANTIC_DATA_CONVERTER,
        interceptors=[_MockTraceContextHeaderInterceptor()],
    )
    wf_id = f"wf-test-interceptor-{uuid.uuid4()}"
    # When
    res = await client.execute_workflow(
        _TestTraceContentWorkflow, id=wf_id, task_queue=TestTaskQueue.WORKFLOWS
    )
    # Then
    assert len(res) == 3
    first = res[0]
    assert first == _DUMMY_TRACE_CTX
    remaining = res[1:]
    for trace_ctx in remaining:
        assert trace_ctx.trace_id == _DUMMY_TRACE_CTX.trace_id
        assert len(trace_ctx.parent_id) == 16
        assert trace_ctx.sampled


async def test_progress_interceptor(
    test_wf_worker,  # noqa: ANN001, ARG001
    test_progress_interceptor_sync_worker,  # noqa: ANN001, ARG001
    test_progress_interceptor_async_worker,  # noqa: ANN001, ARG001
    test_worker_config: WorkerConfig,
) -> None:
    # Given
    temporal_config = test_worker_config.temporal
    client = await TemporalClient.connect(
        target_host=temporal_config.host,
        namespace=temporal_config.namespace,
        data_converter=PYDANTIC_DATA_CONVERTER,
    )
    wf_id = f"wf-test-progress-{uuid.uuid4()}"
    # When
    await client.execute_workflow(
        _TestProgressWorkflow,
        args=[ProgressArg(name="world")],
        id=wf_id,
        task_queue=TestTaskQueue.WORKFLOWS,
    )
    # Then
    wf = client.get_workflow_handle(workflow_id=wf_id)
    search_attributes = (await wf.describe()).search_attributes
    max_progress = search_attributes.get("MaxProgress")[0]
    assert max_progress == 6.0
    progress = search_attributes.get("Progress")[0]
    assert progress == 6.0


async def test_heartbeat_interceptor(
    test_heartbeat_interceptor_worker,  # noqa: ANN001, ARG001
    test_worker_config: WorkerConfig,
) -> None:
    # Given
    temporal_config = test_worker_config.temporal
    client = await TemporalClient.connect(
        target_host=temporal_config.host,
        namespace=temporal_config.namespace,
        data_converter=PYDANTIC_DATA_CONVERTER,
    )
    wf_id = f"wf-test-heartbeat-{uuid.uuid4()}"
    # When
    res = await client.execute_workflow(
        _TestHeartbeatWorkflow, id=wf_id, task_queue=TestTaskQueue.HEARTBEAT
    )
    assert res is None


async def test_heartbeat_interceptor_should_fail_when_no_heartbeat(
    test_heartbeat_interceptor_no_heartbeat_worker,  # noqa: ANN001, ARG001
    test_worker_config: WorkerConfig,
) -> None:
    # Given
    temporal_config = test_worker_config.temporal
    client = await TemporalClient.connect(
        target_host=temporal_config.host,
        namespace=temporal_config.namespace,
        data_converter=PYDANTIC_DATA_CONVERTER,
    )
    wf_id = f"wf-test-heartbeat-{uuid.uuid4()}"
    # When
    with pytest.raises(WorkflowFailureError) as ctx:
        await client.execute_workflow(
            _TestNoHeartbeatWorkflow, id=wf_id, task_queue=TestTaskQueue.NO_HEARTBEAT
        )
    cause = ctx.value.cause.__cause__
    assert isinstance(cause, temporalio_exceptions.TimeoutError)
    assert "Heartbeat timeout" in cause.args[0]
