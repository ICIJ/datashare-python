# --8<-- [start:setup]
from datashare_python.constants import DATA_DIR
from exceptions import DependencyInjectionError
from icij_common.logging_utils import setup_loggers
from lancedb import AsyncConnection as LanceDBConnection
from lancedb import connect_async

_VECTOR_DB_CONNECTION: LanceDBConnection | None = None
_DB_PATH = DATA_DIR / "vector.db"


async def vector_db_setup(**_) -> None:
    global _VECTOR_DB_CONNECTION
    _VECTOR_DB_CONNECTION = await connect_async(_DB_PATH)


# --8<-- [end:setup]
# --8<-- [start:provide]
def lifespan_vector_db() -> LanceDBConnection:
    if _VECTOR_DB_CONNECTION is None:
        raise DependencyInjectionError("vector db connection")
    return _VECTOR_DB_CONNECTION


# --8<-- [end:provide]
# --8<-- [start:teardown]
async def vector_db_teardown(exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
    await lifespan_vector_db().__aexit__(exc_type, exc_val, exc_tb)
    global _VECTOR_DB_CONNECTION
    _VECTOR_DB_CONNECTION = None


# --8<-- [end:teardown]
# --8<-- [start:registry]
# Register all dependencies in the format of:
# (<logging helper>, <dep setup>, <dep teardown>)
APP_LIFESPAN_DEPS = [
    ("loading async app configuration", load_app_config, None),  # noqa: F821
    ("loggers", setup_loggers, None),
    ("elasticsearch client", es_client_setup, es_client_teardown),  # noqa: F821
    ("task client", task_client_setup, task_client_teardown),  # noqa: F821
    ("vector db connection", vector_db_setup, vector_db_teardown),
]
# --8<-- [end:registry]
