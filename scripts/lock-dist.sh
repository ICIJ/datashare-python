#!/bin/bash
set -e

if [[ "$1" == "worker-template" ]];then
    cd "$1"
else
    cd workers/"$1"
fi

# uv bug -n flag to discard cache takes 2 times to work
uv lock -n --no-sources-package datashare-python || uv lock -n --no-sources-package datashare-python
cp uv.lock uv.dist.lock
uv lock