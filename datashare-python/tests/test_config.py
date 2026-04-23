import os

from datashare_python.config import WorkerConfig


def test_worker_config_loggers_from_env(reset_env) -> None:  # noqa: ANN001, ARG001
    # Given
    loggers = '{"datashare_python": "WARNING"}'
    os.environ["DS_WORKER_LOGGING__LOGGERS"] = loggers
    # When
    config = WorkerConfig()
    # Then
    assert config.logging.loggers["datashare_python"] == "WARNING"
