import os
from unittest.mock import AsyncMock, patch

import pytest
from datashare_python.config import (
    WorkerConfig,
)
from pydantic import ValidationError


def test_worker_config_loggers_from_env(reset_env) -> None:  # noqa: ANN001, ARG001
    # Given
    loggers = '{"datashare_python": "WARNING"}'
    os.environ["DS_WORKER_LOGGING__LOGGERS"] = loggers
    # When
    config = WorkerConfig()
    # Then
    assert config.logging.loggers["datashare_python"] == "WARNING"


async def test_worker_config_should_export_prometheus_metrics(reset_env) -> None:  # noqa: ANN001, ARG001
    # Given
    prometheus_host = "0.0.0.0:9000"
    os.environ["DS_WORKER_TEMPORAL__PROMETHEUS_HOST"] = prometheus_host
    config = WorkerConfig()
    assert config.temporal.prometheus_host == prometheus_host
    # When
    mock_connect = AsyncMock()
    with patch("datashare_python.config.TemporalClient.connect", mock_connect):
        await config.temporal.to_client()
    # Then
    assert mock_connect.await_args_list[0].kwargs["runtime"] is not None


def test_worker_config_should_raise_for_invalid_property(reset_env) -> None:  # noqa: ANN001, ARG001
    # Given
    os.environ["DS_WORKER_TEMPORAL__IDONT"] = "exist"
    # When/Then
    expected = "Extra inputs are not permitted"
    with pytest.raises(ValidationError, match=expected):
        WorkerConfig()
