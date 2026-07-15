#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies extract.extract \
    --queue extract.gpu.mineru \
    --activity extract.extract-markdown-content
