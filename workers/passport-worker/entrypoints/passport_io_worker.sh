#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies passport-detection.io \
    --queue passport-detection.io \
    --activities passport-detection.create-preprocessing-batches \
    --activities passport-detection.convert-to-pdf \
    --activities passport-detection.preprocess.pdfs \
    --activities passport-detection.create-inference-batches \
    --activities passport-detection.aggregate-results