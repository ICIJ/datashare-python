#!/bin/bash
N_PROCESSING_WORKERS=
uv run python -m icij_worker workers start -g PYTHON -n "${N_PROCESSING_WORKERS:-1}" datashare_python.app.app
