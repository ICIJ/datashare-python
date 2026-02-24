from typing import Optional

from icij_worker import AsyncApp
from icij_worker.typing_ import PercentProgress
from pydantic import parse_obj_as

from datashare_python.constants import PYTHON_TASK_GROUP
from datashare_python.objects import ClassificationConfig, TranslationConfig
from datashare_python.tasks import (
    classify_docs as classify_docs_,
    create_classification_tasks as create_classification_tasks_,
    create_translation_tasks as create_translation_tasks_,
    translate_docs as translate_docs_,
)
from datashare_python.tasks.dependencies import APP_LIFESPAN_DEPS

app = AsyncApp("ml", dependencies=APP_LIFESPAN_DEPS)


@app.task(group=PYTHON_TASK_GROUP)
async def create_translation_tasks(
    project: str,
    target_language: str,
    config: dict | None = None,
    user: dict | None = None,  # pylint: disable=unused-argument
) -> list[str]:
    # Parse the incoming config
    config = parse_obj_as(Optional[TranslationConfig], config)
    return await create_translation_tasks_(
        project=project, target_language=target_language, config=config
    )


@app.task(group=PYTHON_TASK_GROUP)
async def translate_docs(
    docs: list[str],
    project: str,
    target_language: str,
    progress: PercentProgress,
    config: dict | None = None,
    user: dict | None = None,  # pylint: disable=unused-argument
) -> int:
    config = parse_obj_as(Optional[TranslationConfig], config)
    return await translate_docs_(
        docs, target_language, project=project, config=config, progress=progress
    )


@app.task(group=PYTHON_TASK_GROUP)
async def create_classification_tasks(
    project: str,
    language: str,
    n_workers: int,
    progress: PercentProgress,
    config: dict | None = None,
    user: dict | None = None,  # pylint: disable=unused-argument
) -> list[str]:
    config = parse_obj_as(Optional[ClassificationConfig], config)
    return await create_classification_tasks_(
        project=project,
        language=language,
        n_workers=n_workers,
        config=config,
        progress=progress,
    )


@app.task(group=PYTHON_TASK_GROUP)
async def classify_docs(
    docs: list[str],
    language: str,
    project: str,
    progress: PercentProgress,
    config: dict | None = None,
    user: dict | None = None,  # pylint: disable=unused-argument
) -> int:
    config = parse_obj_as(Optional[ClassificationConfig], config)
    return await classify_docs_(
        docs, language=language, project=project, config=config, progress=progress
    )


@app.task(group=PYTHON_TASK_GROUP)
def ping() -> str:
    return "pong"
