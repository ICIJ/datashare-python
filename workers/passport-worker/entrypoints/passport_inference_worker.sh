#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies passport-detection.inference \
    --queue passport-detection.inference \
    --activities passport-detection.preprocess.images