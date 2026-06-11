#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies extract.io \
    --queue extract.io \
    --activity extract.worker-config \
    --activity extract.create-markdown-batches