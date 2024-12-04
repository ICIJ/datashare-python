from typing import Dict, Optional, Union

from icij_worker import AsyncApp
from icij_worker.app import TaskGroup
from icij_worker.typing_ import PercentProgress
from pydantic import parse_obj_as

from ml_worker.objects import ClassificationConfig, TranslationConfig
from ml_worker.tasks.dependencies import (
    APP_LIFESPAN_DEPS,
    lifespan_es_client,
)
from ml_worker.tasks import (
    translate_docs as translate_docs_,
    classify_docs as classify_docs_,
)

# We define an `AsyncApp` which acts as a registry to register Python functions as task
# which can be executed by the asynchronous Python workers.
# We also inject dependencies in the app, these dependencies will be injected when the
# apps starts up
app = AsyncApp("ml", dependencies=APP_LIFESPAN_DEPS)

# We use the Python TaskGroup when registering the task so that Datashare route the
# following task to Python workers and not to other workers (java for instance)
PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")


# Let's register two different tasks. The task functions are thin wrappers around
# "core" functions, they just deserialize arguments and forward them to the core
# function. Doing so helps with testing as you can focus on testing the core and not
# the serialization/deserialization stuff
@app.task(group=PYTHON_TASK_GROUP)
async def classify_docs(
    config: Optional[Dict],
    # We add a progress function to allow us to send some progress udpates
    progress: PercentProgress,
    # We have to add the user group as Datashare adds it to all task arguments
    user: dict,  # pylint: disable=unused-argument
) -> int:
    # Parse the incoming classification config
    config = parse_obj_as(Union[ClassificationConfig, None], config)
    # Load the global ES client created through deps injection
    es_client = lifespan_es_client()
    return await classify_docs_(es_client, config, progress)


@app.task(group=PYTHON_TASK_GROUP)
async def translate_docs(
    target_language: str,
    config: Optional[Dict],
    progress: PercentProgress,
    # We have to add the user group as Datashare adds it to all task arguments
    user: dict,  # pylint: disable=unused-argument
) -> int:
    base_model_config = parse_obj_as(Union[TranslationConfig, None], config)
    # Parse the incoming classification config
    config = parse_obj_as(Union[ClassificationConfig, None], config)
    if config is None:
        config = ClassificationConfig()
    # Load the global ES client created through deps injection
    es_client = lifespan_es_client()
    return await translate_docs_(
        target_language, es_client, base_model_config, progress
    )
