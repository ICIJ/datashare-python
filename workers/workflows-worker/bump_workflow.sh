#!/bin/bash
set -e
worker_lib="$1"

if [[ "$worker_lib" == "datashare-python" ]]; then
    op="~="
else
    op="=="
fi

uv add -n "$1$op$2" || uv add -n "$1$op$2"
git add pyproject.toml uv.lock
git commit -m "chore(workflows-worker): bump $1 to $2"
