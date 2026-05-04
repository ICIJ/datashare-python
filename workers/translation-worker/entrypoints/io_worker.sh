#!/bin/bash
set -e

uv run --no-sync datashare-python worker start \
    --dependencies io \
    --queue translation.io \
    --activities translation.worker_config \
    --activities translation.create_translation_batches \
    --workflows translation
