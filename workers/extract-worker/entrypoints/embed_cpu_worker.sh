#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies extract.embed \
    --queue extract.embed.cpu \
    --activity extract.embed-documents
