#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue worker-template.translate-gpu \
    --activities "translate-docs"
