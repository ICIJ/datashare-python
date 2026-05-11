#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue datashare.workflows \
    --workflow asr.transcription \
    --workflow translation
