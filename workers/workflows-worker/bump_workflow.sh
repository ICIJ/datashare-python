#!/bin/bash
set -e
worker_lib="$1"

if [[ "$worker_lib" == "datashare-python" ]]; then
    op="~="
else
    op="=="
fi
uv add -n "$1$op$2" || uv add -n "$1$op$2"
cd ../..
make lock-dist project=workflows-worker
cd workers/worker
git add uv.lock uv.dist.lock pyproject.toml
git commit -m "chore(workflows-worker): bump $1$op$2"
