#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies cpu \
    --queue asr.cpu \
    --activities asr.transcription.preprocess \
    --activities asr.transcription.postprocess
