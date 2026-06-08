#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies translation.io \
    --queue translation.io \
    --activity translation.worker-config \
    --activity translation.create-translation-batches