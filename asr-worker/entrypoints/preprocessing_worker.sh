#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue asr.preprocessing.cpu \
    --activities asr.transcription.preprocess
