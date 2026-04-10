#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies io \
    --queue asr.io \
    --activities asr.transcription.search-audios \
    --workflows asr.transcription
