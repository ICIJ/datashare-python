#!/bin/bash
set -e
projects=(
    caul
    caul-core
    datashare-python
    worker-template
    workers/asr-worker
    workers/extract-worker
    workers/passport-worker
    workers/translation-worker
    workers/workflows-worker
)
root=$(dirname $(dirname "$(realpath $0)"))
for p in ${projects[@]}; do
    echo "Formatting $p..."
    cd "$p"
    uv run ruff check --fix .
    cd "$root"
done
