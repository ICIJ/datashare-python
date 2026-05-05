#!/bin/bash

uv run --no-sync datashare-python worker start \
    --dependencies inference \
    --queue translation.inference.cpu \
    --activities translation.translate_docs
