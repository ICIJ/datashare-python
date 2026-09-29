#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies asr.io \
    --queue asr.io \
    --activity asr.transcription.config \
    --activity asr.transcription.search-audios \
    --activity asr.transcription.aggregate-results \
    --activity asr.transcription.index