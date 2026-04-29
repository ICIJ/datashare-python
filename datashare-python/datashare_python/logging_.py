import logging
import sys
from copy import copy

from icij_common.logging_utils import DATE_FMT, STREAM_HANDLER_FMT
from pythonjsonlogger.core import RESERVED_ATTRS, BaseJsonFormatter
from pythonjsonlogger.orjson import OrjsonFormatter
from temporalio import activity, workflow

from .config import LogLevel
from .interceptors import get_trace_context

_ACT_LOGGER_ATTRS = ["activity_type", "activity_id", "activity_run_id"]
_WF_LOGGED_ATTRS = ["workflow_type", "workflow_id", "workflow_run_id"]
_TRACE_CONTEXT_ATTRS = ["trace_id", "parent_id", "traceparent"]
_LOGGED_ATTRIBUTES = (
    copy(RESERVED_ATTRS)
    + _WF_LOGGED_ATTRS
    + _ACT_LOGGER_ATTRS
    + _TRACE_CONTEXT_ATTRS
    + ["worker_id"]
)


_STREAM_HANDLER_FMT_WITH_WORKER_ID = (
    "[%(levelname)s][%(asctime)s.%(msecs)03d][%(worker_id)s][%(name)s]: %(message)s"
)


def setup_worker_loggers(
    loggers: dict[str, LogLevel], *, worker_id: str | None, in_json: bool
) -> None:
    worker_filter = WorkerFilter(worker_id)
    for logger_name, level_str in loggers.items():
        level = getattr(logging, level_str)
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.handlers = []
        for handler in _get_worker_handlers(level, worker_filter, in_json=in_json):
            logger.addHandler(handler)


class WorkerFilter(logging.Filter):
    def __init__(self, worker_id: str | None) -> None:
        super().__init__()
        self.worker_id = worker_id

    def filter(self, record: logging.LogRecord) -> bool:
        if self.worker_id is not None:
            record.worker_id = self.worker_id
        if workflow.in_workflow():
            wf_info = workflow.info()
            for attr in _WF_LOGGED_ATTRS:
                setattr(record, attr, getattr(wf_info, attr))
        if activity.in_activity():
            act_info = activity.info()
            for attr in _ACT_LOGGER_ATTRS:
                setattr(record, attr, getattr(act_info, attr))
        trace_context = get_trace_context()
        if trace_context is not None:
            for attr in _TRACE_CONTEXT_ATTRS:
                setattr(record, attr, getattr(trace_context, attr))
        return True


def _get_worker_handlers(
    level: int, worker_filter: WorkerFilter, *, in_json: bool
) -> list[logging.Handler]:
    stream_handler = logging.StreamHandler(sys.stderr)
    if in_json:
        fmt = _json_formatter(datefmt=DATE_FMT)
    else:
        if worker_filter.worker_id is not None:
            fmt = _STREAM_HANDLER_FMT_WITH_WORKER_ID
        else:
            fmt = STREAM_HANDLER_FMT
        fmt = logging.Formatter(fmt, DATE_FMT)
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(level)
    stream_handler.addFilter(worker_filter)
    return [stream_handler]


def _json_formatter(datefmt: str) -> BaseJsonFormatter:
    fmt = OrjsonFormatter(  # let's keep logging as fast as possible
        _LOGGED_ATTRIBUTES, datefmt=datefmt
    )
    return fmt
