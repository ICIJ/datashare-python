from datashare_python.dependencies import (
    lifespan_es_client,  # noqa: F401
    set_es_client,
    set_loggers,
    set_worker_config,
)

IO = [set_worker_config, set_loggers, set_es_client]
EXTRACT = [set_worker_config, set_loggers]

DEPENDENCIES = {"extract.io": IO, "extract.extract": EXTRACT}
