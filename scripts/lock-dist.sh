#!/bin/bash
set -e

cd "$1"
# uv bug -n flag to discard cache takes 2 times to work
uv lock -n --no-sources-package datashare-python || uv lock -n --no-sources-package datashare-python
cp uv.lock uv.dist.lock
uv lock