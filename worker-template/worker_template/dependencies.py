from datashare_python.dependencies import (
    set_es_client,
    set_event_loop,
    set_loggers,
    set_temporal_client,
)

BASE = [set_loggers, set_event_loop, set_es_client, set_temporal_client]

DEPENDENCIES = {"base": BASE}
