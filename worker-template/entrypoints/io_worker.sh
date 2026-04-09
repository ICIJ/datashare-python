#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue worker-template.io \
    --activities create-.* \
    --workflows translate-and-classify
