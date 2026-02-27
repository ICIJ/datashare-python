import nest_asyncio
from datashare_python.conftest import *  # noqa: F403


@pytest.fixture  # noqa: F405
def typer_asyncio_patch() -> None:
    nest_asyncio.apply()
