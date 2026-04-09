#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue asr.preprocessing.io \
    --activities asr.transcription.search-audios \
    --workflows asr.transcription \
    --dependencies io
