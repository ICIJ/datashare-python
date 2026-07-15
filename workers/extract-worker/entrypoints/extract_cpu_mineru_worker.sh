#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies extract.extract \
    --queue extract.cpu.mineru \
    --activity extract.extract-markdown-content
