#!/bin/bash
set -e

uv add "$1==$2"
git add pyproject.toml uv.lock
git commit -m "chore(workflows-worker): bump $1 to $2"
