import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pytest
import temporalio

with temporalio.workflow.unsafe.imports_passed_through():
    from datashare_python.config import PYDANTIC_DATA_CONVERTER, WorkerConfig
    from datashare_python.interceptors import (
        TraceContext,
        TraceContextInterceptor,
        get_trace_context,
    )
    from datashare_python.types_ import TemporalClient
    from temporalio import activity, workflow
    from temporalio.client import (
        Interceptor,
        OutboundInterceptor,
        StartWorkflowInput,
        WorkflowHandle,
    )
    from temporalio.converter import DataConverter
    from temporalio.worker import Worker

_TEST_CTX_QUEUE = "test.ctx.queue"

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


_TIMEOUT = timedelta(seconds=10)


@workflow.defn
class _TestTraceContentWorkflow:
    @workflow.run
    async def run(self) -> list[TraceContext]:
        current_ctx = get_trace_context()
        ctx_log = [current_ctx]
        ctx_log = await workflow.execute_activity(
            ctx_test_act,
            ctx_log,
            task_queue=_TEST_CTX_QUEUE,
            start_to_close_timeout=_TIMEOUT,
        )
        ctx_log = await workflow.execute_activity(
            ctx_test_act,
            ctx_log,
            task_queue=_TEST_CTX_QUEUE,
            start_to_close_timeout=_TIMEOUT,
        )
        return ctx_log


@activity.defn
async def ctx_test_act(previous: list[TraceContext]) -> list[TraceContext]:
    previous.append(get_trace_context())
    return previous


@pytest.fixture(scope="session")
async def test_interceptor_worker(
    test_temporal_client_session: TemporalClient,
) -> AsyncGenerator[None, None]:
    client = test_temporal_client_session
    worker_id = f"test-interceptor-worker-{uuid.uuid4()}"
    interceptors = [TraceContextInterceptor()]
    worker = Worker(
        client,
        identity=worker_id,
        activities=[ctx_test_act],
        workflows=[_TestTraceContentWorkflow],
        interceptors=interceptors,
        task_queue=_TEST_CTX_QUEUE,
    )
    async with worker:
        yield


async def test_trace_context_interceptor(
    test_interceptor_worker,  # noqa: ANN001, ARG001
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
        _TestTraceContentWorkflow, id=wf_id, task_queue=_TEST_CTX_QUEUE
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
