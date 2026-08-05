#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies passport-detection.io \
    --queue passport-detection.io \
    --activity extract.worker-config \
    --activity extract.create-markdown-batches