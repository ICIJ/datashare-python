import pytest  # noqa: I001
import nest_asyncio
from datashare_python.conftest import *  # noqa: F403
from datashare_python.template import build_template_tarball


@pytest.fixture  # noqa: F405
def typer_asyncio_patch() -> None:
    nest_asyncio.apply()


@pytest.fixture(scope="session")
def local_template_build() -> None:
    build_template_tarball()
