import pytest  # noqa: I001
from datashare_python.conftest import *  # noqa: F403
from datashare_python.template import build_template_tarball


@pytest.fixture(scope="session")
def local_template_build() -> None:
    build_template_tarball()
