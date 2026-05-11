#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies asr.io \
    --queue asr.io \
    --activity asr.transcription.search-audios \
    --workflow asr.transcription
