import logging

from icij_common.es import ESClient
from icij_worker import WorkerConfig
from icij_worker.ds_task_client import DatashareTaskClient
from icij_worker.utils.dependencies import DependencyInjectionError

from ml_worker.config import AppConfig

logger = logging.getLogger(__name__)

# Lifespan dependencies consist in global variable which can be loaded in function
# calling lifespan_<dep_name>(), which returns the global variable.
# The variable itself is created and setup in <>_enter function and if needed
# torn down in the <>_exit function.
# The setup and tear down functions are registered in the APP_LIFESPAN_DEPS list which
# is then passed to the AsyncApp when creating it. The app will take care of setup up
# and tearing down all dependencies in the list. Since a dep might depend on another
# one, the order in which they are registered is important.
# We hence start by registering the configuration, other deps are created from it.

_ASYNC_APP_CONFIG: AppConfig | None = None
_ES_CLIENT: ESClient | None = None
_TASK_CLIENT: DatashareTaskClient | None = None


# App loading setup
def load_app_config(worker_config: WorkerConfig, **_):
    global _ASYNC_APP_CONFIG
    if worker_config.app_bootstrap_config_path is not None:
        _ASYNC_APP_CONFIG = AppConfig.parse_file(
            worker_config.app_bootstrap_config_path
        )
    else:
        _ASYNC_APP_CONFIG = AppConfig()


# Returns the globally injected config
def lifespan_config() -> AppConfig:
    if _ASYNC_APP_CONFIG is None:
        raise DependencyInjectionError("config")
    return _ASYNC_APP_CONFIG


# Loggers setup
def setup_loggers(worker_id: str, **_):
    config = lifespan_config()
    config.setup_loggers(worker_id=worker_id)
    logger.info("worker loggers ready to log 💬")
    logger.info("app config: %s", config.json(indent=2))


# Elasticsearch client setup
async def es_client_enter(**_):
    # pylint: disable=unnecessary-dunder-call
    config = lifespan_config()
    global _ES_CLIENT
    _ES_CLIENT = config.to_es_client()
    await _ES_CLIENT.__aenter__()


# Elasticsearch client teardown
async def es_client_exit(exc_type, exc_val, exc_tb):
    # pylint: disable=unnecessary-dunder-call
    await lifespan_es_client().__aexit__(exc_type, exc_val, exc_tb)
    global _ES_CLIENT
    _ES_CLIENT = None


# Returns the globally injected ES client
def lifespan_es_client() -> ESClient:
    # pylint: disable=unnecessary-dunder-call
    if _ES_CLIENT is None:
        raise DependencyInjectionError("es client")
    return _ES_CLIENT


# Task client setup
async def task_client_enter(**_):
    # pylint: disable=unnecessary-dunder-call
    config = lifespan_config()
    global _TASK_CLIENT
    _TASK_CLIENT = config.to_task_client()
    await _TASK_CLIENT.__aenter__()


# Task client teardown
async def task_client_exit(exc_type, exc_val, exc_tb):
    # pylint: disable=unnecessary-dunder-call
    await lifespan_task_client().__aexit__(exc_type, exc_val, exc_tb)
    global _TASK_CLIENT
    _TASK_CLIENT = None


# Returns the globally injected task client
def lifespan_task_client() -> DatashareTaskClient:
    # pylint: disable=unnecessary-dunder-call
    if _TASK_CLIENT is None:
        raise DependencyInjectionError("task client")
    return _TASK_CLIENT


# Register all dependencies in the format of:
# (<logging helper>, <dep setup>, <dep teardown>)
APP_LIFESPAN_DEPS = [
    ("loading async app configuration", load_app_config, None),
    ("loggers", setup_loggers, None),
    ("elasticsearch client", es_client_enter, es_client_exit),
    ("task client", task_client_enter, task_client_exit),
]
