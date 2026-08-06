#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies asr.inference \
    --queue asr.inference.gpu \
    --activity asr.transcription.infer
