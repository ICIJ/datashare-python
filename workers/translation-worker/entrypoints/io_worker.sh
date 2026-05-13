#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies translation.io \
    --queue translation.io \
    --activity translation.worker_config \
    --activity translation.create_translation_batches