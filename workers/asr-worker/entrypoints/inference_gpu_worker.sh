#!/bin/bash

uv run --no-sync datashare-python worker start \
    --dependencies inference \
    --queue worker-template.classify-gpu \
    --activities asr.transcription.infer
