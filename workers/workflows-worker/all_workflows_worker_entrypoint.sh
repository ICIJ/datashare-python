#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --skip-config-discovery \
    --queue datashare.workflows \
    --workflow asr.transcription \
    --workflow translation \
    --workflow extract.markdown
