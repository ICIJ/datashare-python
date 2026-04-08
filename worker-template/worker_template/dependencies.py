from datashare_python.dependencies import (
    lifespan_es_client,  # noqa: F401
    set_es_client,
    set_loggers,
)

BASE = [set_loggers, set_es_client]

DEPENDENCIES = {"base": BASE}
