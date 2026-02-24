#!/bin/bash
TEMPORAL_HOST=${1:-temporal}
TEMPORAL_PORT=${2:-7233}
WORKERS=${3:-'["asr_worker.worker.run_asr_worker"]'}

datashare-python local start-workers --temporal-address $TEMPORAL_HOST:$TEMPORAL_PORT --worker-paths $WORKERS