import nest_asyncio
import pytest


@pytest.fixture
def typer_asyncio_patch() -> None:
    nest_asyncio.apply()
