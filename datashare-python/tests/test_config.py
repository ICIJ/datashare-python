import os
from unittest.mock import AsyncMock, patch

from datashare_python.config import (
    WorkerConfig,
)


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
