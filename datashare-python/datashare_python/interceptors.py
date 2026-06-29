import asyncio
import contextlib
import dataclasses
import secrets
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from functools import partial, wraps
from inspect import signature
from types import UnionType
from typing import (
    Annotated,
    Any,
    NoReturn,
    Self,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from nexusrpc import InputT, OutputT
from pydantic import Field
from temporalio import activity
from temporalio.activity import _Definition
from temporalio.api.common.v1 import Payload
from temporalio.client import WorkflowHandle
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
from .types_ import (
    AsyncProgressRateHandler,
    ProgressRateHandler,
    SyncProgressRateHandler,
    Weight,
)
from .utils import (
    PROGRESS_HANDLER_ARG,
    PYDANTIC_DATA_CONVERTER,
    ActivityWithProgress,
    ProgressSignal,
)

_TRACEPARENT = "traceparent"
_DEFAULT_PAYLOAD_CONVERTER = DataConverter.default.payload_converter
_PROGRESS_TYPES = {
    ProgressRateHandler,
    AsyncProgressRateHandler,
    SyncProgressRateHandler,
}


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


class ProgressInterceptor(Interceptor):
    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,  # noqa: A002
    ) -> ActivityInboundInterceptor:
        return _ProgressInboundInterceptor(next)


def _parse_progress_weight(act_fn: Callable) -> float:
    hints = get_type_hints(act_fn, include_extras=True)
    hint = hints["progress"]
    annotated_progress = get_origin(hint)
    if annotated_progress is not Annotated:
        return 1.0
    annotated_args = get_args(hint)
    for ann in annotated_args[1:]:
        if isinstance(ann, Weight):
            return ann.value
    return 1.0


async def progress_handler(
    progress: float,
    handle: WorkflowHandle,
    *,
    activity_id: str,
    run_id: str,
    weight: float = 1.0,
) -> None:
    signal = ProgressSignal(
        activity_id=activity_id, run_id=run_id, progress=progress, weight=weight
    )
    await handle.signal("update_progress", signal)


def supports_progress(task_fn: Callable) -> bool:
    return any(
        param.name == PROGRESS_HANDLER_ARG
        for param in signature(task_fn).parameters.values()
    )


def _get_progress_handler(act_fn: Callable) -> ProgressRateHandler:
    act = getattr(act_fn, "__self__", None)
    # Weirdly isinstance doesn't work here
    if act is None or not isinstance(act, ActivityWithProgress):
        msg = (
            f"to support progress, activities should inherit from "
            f"{ActivityWithProgress.__name__}."
        )
        raise TypeError(msg)
    weight = _parse_progress_weight(act_fn)
    info = activity.info()
    run_id = info.workflow_run_id
    workflow_id = info.workflow_id
    activity_id = activity.info().activity_id
    client = act._temporal_client
    workflow_handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    handler = partial(
        progress_handler,
        handle=workflow_handle,
        run_id=run_id,
        activity_id=activity_id,
        weight=weight,
    )
    return handler


def _is_progress(t: type) -> bool:
    if any(t is p_cls for p_cls in _PROGRESS_TYPES):
        return True
    return bool(
        isinstance(t, UnionType)
        and any(
            any(sub_t is p_cls for p_cls in _PROGRESS_TYPES) for sub_t in get_args(t)
        )
    )


def _without_progress(arg_types: list[type] | None) -> list[type] | None:
    if arg_types is None:
        return None
    filtered = [t for t in arg_types if not _is_progress(t)]
    return filtered


class _ProgressInboundInterceptor(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:  # noqa: A002
        if not supports_progress(input.fn):
            return await super().execute_activity(input)
        # The progress args breaks trigger a bypass of the dataloader:
        # https://github.com/temporalio/sdk-python/blob/631ebaf0e20fb214b16589b45627b358048a5d77/temporalio/worker/_activity.py#L600
        # we have to force it here again
        progress_handler = _get_progress_handler(input.fn)
        new_args = []
        act_definition = _Definition.must_from_callable(input.fn)
        if input.args:
            data_converter = PYDANTIC_DATA_CONVERTER
            arg_types = act_definition.arg_types
            arg_types = _without_progress(arg_types)
            arg_types = arg_types[: len(input.args)]
            encoded = await data_converter.encode(input.args)
            new_args = await data_converter.decode(encoded, type_hints=arg_types)
        injected_progress = (
            progress_handler
            if act_definition.is_async
            else _sync_progress(progress_handler)
        )
        new_args.append(injected_progress)
        new_input = dataclasses.replace(input, args=new_args)
        await progress_handler(0.0)
        res = await super().execute_activity(new_input)
        await progress_handler(1.0)
        return res


class HeartbeatInterceptor(Interceptor):
    def __init__(self, n_missed_before_timeout: int = 5):
        self._n_missed_before_timeout = n_missed_before_timeout

    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,  # noqa: A002
    ) -> ActivityInboundInterceptor:
        return _HeartbeatInboundInterceptor(next, self._n_missed_before_timeout)


async def _heartbeat_every(period: float, *details: Any) -> None:
    with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
        activity.heartbeat(*details)
    while True:
        await asyncio.sleep(period)
        with contextlib.suppress(RuntimeError, asyncio.TimeoutError):
            activity.heartbeat(*details)


class _HeartbeatInboundInterceptor(ActivityInboundInterceptor):
    def __init__(
        self,
        next: ActivityInboundInterceptor,  # noqa: A002
        n_missed_before_timeout: int = 5,
    ) -> None:
        super().__init__(next)
        self._n_missed_before_timeout = n_missed_before_timeout

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:  # noqa: A002
        heartbeat_timeout = activity.info().heartbeat_timeout
        heartbeat_task = None
        if heartbeat_timeout:
            period = heartbeat_timeout.total_seconds() / self._n_missed_before_timeout
            heartbeat_task = asyncio.create_task(_heartbeat_every(period))
        try:
            activity.heartbeat()
            return await super().execute_activity(input)
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                await asyncio.wait([heartbeat_task])


def _sync_progress(
    progress_handler: AsyncProgressRateHandler,
) -> SyncProgressRateHandler:
    @wraps(progress_handler)
    def p(progress: float, event_loop: asyncio.AbstractEventLoop) -> None:
        asyncio.run_coroutine_threadsafe(
            progress_handler(progress), event_loop
        ).result()

    return p
