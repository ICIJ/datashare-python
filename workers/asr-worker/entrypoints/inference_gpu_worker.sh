#!/bin/bash

uv run --no-sync datashare-python worker start \
    --dependencies inference \
    --queue asr.inference.gpu \
    --activities asr.transcription.infer
