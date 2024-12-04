import logging

from icij_common.es import ESClient
from icij_worker import WorkerConfig
from icij_worker.utils.dependencies import DependencyInjectionError

from ml_worker.config import AppConfig

logger = logging.getLogger(__name__)

_ASYNC_APP_CONFIG: AppConfig | None = None
_ES_CLIENT: ESClient | None = None


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


# Register all dependencies
APP_LIFESPAN_DEPS = [
    ("loading async app configuration", load_app_config, None),
    ("loggers", setup_loggers, None),
    ("elasticsearch client", es_client_enter, es_client_exit),
]
