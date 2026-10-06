#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies translation.inference \
    --queue translation.inference.c2translate \
    --activity translation.translate-docs
