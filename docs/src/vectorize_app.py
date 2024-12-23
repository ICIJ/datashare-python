# --8<-- [start:vectorize-app]
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
from datashare_python.tasks.vectorize import (
    create_vectorization_tasks as create_vectorization_tasks_,
    find_most_similar as find_most_similar_,
    vectorize_docs as vectorization_docs_,
)

app = AsyncApp("ml", dependencies=APP_LIFESPAN_DEPS)


@app.task(group=PYTHON_TASK_GROUP)
async def create_vectorization_tasks(
    project: str, model: str = "BAAI/bge-small-en-v1.5"
) -> list[str]:
    return await create_vectorization_tasks_(project, model=model)


@app.task(group=PYTHON_TASK_GROUP)
async def vectorization_docs(docs: list[str], project: str) -> int:
    return await vectorization_docs_(docs, project)


@app.task(group=PYTHON_TASK_GROUP)
async def find_most_similar(
    queries: list[str], model: str, n_similar: int = 2
) -> list[list[dict]]:
    return await find_most_similar_(queries, model, n_similar=n_similar)


# --8<-- [end:vectorize-app]
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
