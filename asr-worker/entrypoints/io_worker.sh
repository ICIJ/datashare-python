#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --queue asr.io \
    --activities asr.transcription.search-audios \
    --workflows asr.transcription \
    --dependencies io
