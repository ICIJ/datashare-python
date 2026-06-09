#!/bin/bash
set -e
# TODO: use regex to automatically add new workflows from the same app
uv run --no-sync datashare-python worker start \
    --skip-config-discovery \
    --queue datashare.workflows \
    --workflow asr.transcription \
    --workflow translation \
    --workflow extract.markdown
