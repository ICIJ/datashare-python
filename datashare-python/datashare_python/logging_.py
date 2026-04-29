import logging
import numbers
import sys
from copy import copy
from typing import Any

import orjson
from icij_common.logging_utils import DATE_FMT, STREAM_HANDLER_FMT
from pythonjsonlogger.core import BaseJsonFormatter
from pythonjsonlogger.orjson import OrjsonFormatter
from temporalio import activity, workflow

from .config import LogFormat, LogLevel
from .interceptors import get_trace_context

_BASE_ATTRS = [
    "asctime",
    "exc_info",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
]
_ACT_LOGGER_ATTRS = ["activity_type", "activity_id", "activity_run_id"]
_WF_LOGGED_ATTRS = ["workflow_type", "workflow_id", "workflow_run_id"]
_TRACE_CONTEXT_ATTRS = ["trace_id", "parent_id", "traceparent"]

_LOGGED_ATTRIBUTES = (
    copy(_BASE_ATTRS)
    + _WF_LOGGED_ATTRS
    + _ACT_LOGGER_ATTRS
    + _TRACE_CONTEXT_ATTRS
    + ["worker_id"]
)


_STREAM_HANDLER_FMT_WITH_WORKER_ID = (
    "[%(levelname)s][%(asctime)s.%(msecs)03d][%(worker_id)s][%(name)s]: %(message)s"
)


def setup_worker_loggers(
    loggers: dict[str, LogLevel], *, worker_id: str | None, log_format: LogFormat
) -> None:
    worker_filter = WorkerFilter(worker_id)
    for logger_name, level_str in loggers.items():
        level = getattr(logging, level_str)
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.handlers = []
        for handler in _get_worker_handlers(
            level, worker_filter, log_format=log_format
        ):
            logger.addHandler(handler)


class WorkerFilter(logging.Filter):
    def __init__(self, worker_id: str | None) -> None:
        super().__init__()
        self.worker_id = worker_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "message") and record.msg is not None:
            setattr(record, "msg", record.msg)  # noqa: B010
        if self.worker_id is not None:
            record.worker_id = self.worker_id
        if workflow.in_workflow():
            wf_info = workflow.info()
            wf_info = {
                "workflow_run_id": wf_info.run_id,
                "workflow_id": wf_info.workflow_id,
                "workflow_type": wf_info.workflow_type,
            }
            for attr in _WF_LOGGED_ATTRS:
                setattr(record, attr, wf_info[attr])
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
    level: int, worker_filter: WorkerFilter, *, log_format: LogFormat
) -> list[logging.Handler]:
    stream_handler = logging.StreamHandler(sys.stderr)
    match log_format:
        case LogFormat.JSON:
            fmt = _json_formatter(datefmt=DATE_FMT)
        case LogFormat.LOGFMT:
            fmt = LogFmtFormatter(datefmt=DATE_FMT)
        case LogFormat.DEFAULT:
            if worker_filter.worker_id is not None:
                fmt = _STREAM_HANDLER_FMT_WITH_WORKER_ID
            else:
                fmt = STREAM_HANDLER_FMT
            fmt = logging.Formatter(fmt, DATE_FMT)
        case _:
            raise NotImplementedError(f"invalid log format: {log_format}")
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(level)
    stream_handler.addFilter(worker_filter)
    return [stream_handler]


class LogFmtFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        logged = dict()
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
            logged["exc_info"] = record.exc_text
        for k, v in record.__dict__.items():
            if k in _LOGGED_ATTRIBUTES and k != "exc_info":
                logged[k] = _encode_value(v)
        return " ".join(f"{k}={v}" for k, v in sorted(logged.items()))


def _encode_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, numbers.Number):
        return str(value)
    return orjson.dumps(value).decode()


def _json_formatter(datefmt: str) -> BaseJsonFormatter:
    fmt = OrjsonFormatter(  # let's keep logging as fast as possible
        _LOGGED_ATTRIBUTES, datefmt=datefmt
    )
    return fmt
