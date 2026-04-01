from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    set_es_client,
)

DEPENDENCIES = {
    "base": [set_es_client],
}
