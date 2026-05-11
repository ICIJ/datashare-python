#!/bin/bash

uv run --no-sync datashare-python worker start \
    --dependencies translation.inference \
    --queue translation.inference.cpu \
    --activity translation.translate_docs
