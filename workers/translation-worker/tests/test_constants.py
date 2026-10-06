import re
from pathlib import Path

import pytest
from translation_worker.constants import TaskQueue
from translation_worker.objects import (
    ArgosTranslatorConfig,
    HunyuanMtTranslatorConfig,
    TranslationConfig,
)


@pytest.mark.parametrize(
    ("config", "expected_queue"),
    [
        (
            TranslationConfig(translator=ArgosTranslatorConfig()),
            TaskQueue.C2TRANSLATE_INFERENCE,
        ),
        (
            TranslationConfig(translator=HunyuanMtTranslatorConfig()),
            TaskQueue.TORCH_INFERENCE,
        ),
    ],
)
def test_inference_queue(config: TranslationConfig, expected_queue: TaskQueue) -> None:
    # When
    inference_queue = TaskQueue.inference_queue(config)
    # Then
    assert inference_queue == expected_queue


_WORKER_ROOT = Path(__file__).parents[1]


def _entrypoint_queue(docker_target: str) -> str:
    dockerfile = (_WORKER_ROOT / "Dockerfile").read_text()
    stage = dockerfile.split(f" AS {docker_target}\n", 1)[1].split("\nFROM ", 1)[0]
    entrypoint = re.search(r'ENTRYPOINT \["(.+?)"\]', stage).group(1)
    script = (_WORKER_ROOT / entrypoint).read_text()
    return re.search(r"--queue\s+(\S+)", script).group(1)


@pytest.mark.parametrize(
    ("docker_target", "expected_queue"),
    [
        ("io-worker", TaskQueue.IO),
        ("inference-c2translate-worker", TaskQueue.C2TRANSLATE_INFERENCE),
        ("inference-torch-worker", TaskQueue.TORCH_INFERENCE),
    ],
)
def test_docker_entrypoints_listen_to_workflow_queues(
    docker_target: str, expected_queue: TaskQueue
) -> None:
    # When
    queue = _entrypoint_queue(docker_target)
    # Then
    assert queue == expected_queue
