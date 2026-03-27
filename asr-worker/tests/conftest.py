import os
from pathlib import Path

import pytest
from asr_worker.config import ASRWorkerConfig
from datashare_python.conftest import (  # noqa: F401
    event_loop,
    test_deps,
    test_temporal_client_session,
    test_worker_config,
    worker_lifetime_deps,
)
from icij_common.test_utils import reset_env  # noqa: F401


@pytest.fixture
def mocked_worker_config_in_env(reset_env, tmp_path: Path) -> ASRWorkerConfig:  # noqa: ANN001, ARG001, F811
    os.environ["DS_WORKER_AUDIO_ROOT"] = str(tmp_path / "audios")
    os.environ["DS_WORKER_ARTIFACT_ROOT"] = str(tmp_path / "artifacts")
    os.environ["DS_WORKER_WORKDIR"] = str(tmp_path / "workdir")
    return ASRWorkerConfig()
