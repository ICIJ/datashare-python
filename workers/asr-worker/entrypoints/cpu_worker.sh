#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies asr.cpu \
    --queue asr.cpu \
    --activity asr.transcription.preprocess \
    --activity asr.transcription.postprocess
