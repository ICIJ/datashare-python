#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies passport-detection.preprocessing \
    --queue passport-detection.preprocessing \
    --activities passport-detection.preprocess.images