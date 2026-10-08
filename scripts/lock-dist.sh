#!/bin/bash
set -e

if [[ "$1" == "worker-template" || "$1" == "datashare-python" ]];then
    cd "$1"
else
    cd workers/"$1"
fi
shift 1

# Local monorepo packages are resolved from PyPI in the dist lock
no_sources=(--no-sources-package datashare-python --no-sources-package caul --no-sources-package caul-core)
# uv bug -n flag to discard cache takes 2 times to work
uv lock "$@" -n "${no_sources[@]}" || uv lock "$@" -n "${no_sources[@]}"
cp uv.lock uv.dist.lock
uv lock