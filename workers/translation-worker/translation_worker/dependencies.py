import logging

from datashare_python.dependencies import (  # noqa: F401
    lifespan_es_client,
    lifespan_worker_config,
    set_es_client,
    set_worker_config,
)

logger = logging.getLogger(__name__)


REGISTRY = {
    "inference": [set_worker_config, set_es_client],
    "io": [set_worker_config, set_es_client],
}
