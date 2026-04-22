import secrets
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from typing import Annotated, Any, NoReturn, Self, TypeVar

from nexusrpc import InputT, OutputT
from pydantic import Field
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter
from temporalio.worker import (
    ActivityInboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    Interceptor,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    StartNexusOperationInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from temporalio.workflow import (
    ActivityHandle,
    ChildWorkflowHandle,
    NexusOperationHandle,
)

from .objects import BaseModel

_TRACEPARENT = "traceparent"
_DEFAULT_PAYLOAD_CONVERTER = DataConverter.default.payload_converter


class TraceContext(BaseModel):
    # https://www.w3.org/TR/trace-context/
    version: Annotated[str, Field(frozen=True)] = "00"
    trace_id: str
    parent_id: str
    sampled: bool = True

    def __hash__(self) -> int:
        return hash((self.trace_id, self.parent_id, self.sampled))

    @classmethod
    def next_span(cls, parent: Self | None) -> Self:
        new_span_id = secrets.token_hex(8)
        if parent is None:
            trace_id = secrets.token_hex(16)
            return TraceContext(trace_id=trace_id, parent_id=new_span_id)
        return TraceContext(
            trace_id=parent.trace_id, parent_id=new_span_id, sampled=parent.sampled
        )

    @property
    def traceparent(self) -> str:
        flags = "01" if self.sampled else "00"
        return f"{self.version}-{self.trace_id}-{self.parent_id}-{flags}"

    @classmethod
    def from_traceparent(cls, traceparent: str) -> Self:
        split = traceparent.split("-")
        if len(split) != 4:
            raise ValueError(f"invalid trace parent: {traceparent}")
        version, trace_id, parent_id, flags = split
        if version != "00":
            msg = (
                f"unsupported trace parent version {version} "
                f"for traceparent {traceparent}"
            )
            raise ValueError(msg)
        sampled = flags == "01"
        return cls(trace_id=trace_id, parent_id=parent_id, sampled=sampled)


_TRACE_CONTEXT: ContextVar[TraceContext | None] = ContextVar(
    "trace_context", default=None
)


class TraceContextInterceptor(Interceptor):
    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,  # noqa: A002, ARG002
    ) -> type[WorkflowInboundInterceptor] | None:
        return _TraceContextWorkflowInboundInterceptor

    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,  # noqa: A002
    ) -> ActivityInboundInterceptor:
        return _TraceContextActivityInboundInterceptor(next)


class _TraceContextWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        with_outbound_trace_ctx = _TraceContextWorkflowOutboundInterceptor(outbound)
        super().init(with_outbound_trace_ctx)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:  # noqa: A002
        with _trace_context(input.headers):
            return await super().execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:  # noqa: A002
        with _trace_context(input.headers):
            return await super().handle_signal(input)

    async def handle_query(self, input: HandleQueryInput) -> Any:  # noqa: A002
        with _trace_context(input.headers):
            return await super().handle_query(input)


class _TraceContextWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:  # noqa: A002
        super().continue_as_new(_with_trace_context_header(input))

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:  # noqa: A002
        return await super().signal_child_workflow(_with_trace_context_header(input))

    async def signal_external_workflow(
        self,
        input: SignalExternalWorkflowInput,  # noqa: A002
    ) -> None:
        return await super().signal_external_workflow(_with_trace_context_header(input))

    def start_activity(self, input: StartActivityInput) -> ActivityHandle[Any]:  # noqa: A002
        return super().start_activity(_with_trace_context_header(input))

    async def start_child_workflow(
        self,
        input: StartChildWorkflowInput,  # noqa: A002
    ) -> ChildWorkflowHandle[Any, Any]:
        return await super().start_child_workflow(_with_trace_context_header(input))

    def start_local_activity(
        self,
        input: StartLocalActivityInput,  # noqa: A002
    ) -> ActivityHandle[Any]:
        return super().start_local_activity(_with_trace_context_header(input))

    async def start_nexus_operation(
        self,
        input: StartNexusOperationInput[InputT, OutputT],  # noqa: A002
    ) -> NexusOperationHandle[OutputT]:
        return await super().start_nexus_operation(_with_trace_context_header(input))


class _TraceContextActivityInboundInterceptor(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:  # noqa: A002
        with _trace_context(input.headers):
            return await super().execute_activity(input)


def get_trace_context() -> TraceContext | None:
    return _TRACE_CONTEXT.get()


@contextmanager
def _trace_context(headers: Mapping[str, Payload]) -> Generator[None, None, None]:
    ctx = headers.get(_TRACEPARENT)
    if ctx is not None:
        ctx = _DEFAULT_PAYLOAD_CONVERTER.from_payloads(
            [headers.get(_TRACEPARENT)], None
        )[0]
        ctx = TraceContext.from_traceparent(ctx)
    else:
        ctx = TraceContext.next_span(None)
    tok = None
    try:
        tok = _TRACE_CONTEXT.set(ctx)
        yield
    finally:
        if tok is not None:
            _TRACE_CONTEXT.reset(tok)


InputWithHeaders = TypeVar("InputWithHeaders")


def _with_trace_context_header(
    input_with_headers: InputWithHeaders,
) -> InputWithHeaders:
    ctx = get_trace_context()
    if ctx is None:
        return input_with_headers
    new_obj = deepcopy(input_with_headers)
    next_ctx = TraceContext.next_span(ctx)
    new_obj.headers[_TRACEPARENT] = _DEFAULT_PAYLOAD_CONVERTER.to_payload(
        next_ctx.traceparent
    )
    return new_obj
