from ..objects import available_models
from .utils import AsyncTyper

_MODELS = "models"

_LIST_AVAILABLE_MODELS_HELP = "list available ASR models by language"

models_app = AsyncTyper(name=_MODELS)


@models_app.async_command(help=_LIST_AVAILABLE_MODELS_HELP)
async def list_available() -> None:
    print(available_models().model_dump_json(indent=2))
