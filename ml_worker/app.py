from typing import Optional

from icij_worker import AsyncApp
from icij_worker.typing_ import PercentProgress
from pydantic import parse_obj_as

from ml_worker.constants import PYTHON_TASK_GROUP
from ml_worker.objects import ClassificationConfig, TranslationConfig
from ml_worker.tasks import (
    classify_docs as classify_docs_,
    create_classification_tasks as create_classification_tasks_,
    create_translation_tasks as create_translation_tasks_,
    translate_docs as translate_docs_,
)
from ml_worker.tasks.dependencies import APP_LIFESPAN_DEPS

# Let's build an async app which allows to translate DS document and classify docs as
# positive or negative (using sentiment analysis). The classification task will only
# process documents of a given language, so we can optionally be translated beforehand.

# icij-worker hasn't built-in support of task batching and dependency yet. This is
# often needed to build sequential tasks and split large tasks into several smaller
# ones which are distributed across workers.
# To mimic this behavior, we create producer task which will split a large task into
# smaller once, which will distributed to workers.

# Since the translation is optional before the classification, we'll keep the
# translation and classification workflow independent. However, for each one of this
# workflow, a publisher task collects relevant documents, put them into batches and
# creates new tasks to process each one of these batches

# We define an `AsyncApp` which acts as a registry to register Python functions as task
# which can be executed by the asynchronous Python workers.
# We also inject dependencies in the app, these dependencies will be injected when the
# apps starts up
app = AsyncApp("ml", dependencies=APP_LIFESPAN_DEPS)

# Task functions are thin wrappers around
# "core" functions, they just deserialize arguments and forward them to the core
# function. Doing so helps with testing as you can focus on testing the core and not
# the serialization/deserialization stuff

# All task have a user argument since Datashare automatically adds it to all task
# arguments, it then forwarded the task function and ignored. We also add a progress
# arg to allow us to send some progress updates

# We use the Python TaskGroup when registering the task so that Datashare route the
# following task to Python workers and not to other workers (java for instance)


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
