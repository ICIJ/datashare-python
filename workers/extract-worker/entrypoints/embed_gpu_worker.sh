#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies extract.embed \
    --queue extract.embed.gpu \
    --activity extract.embed-documents
