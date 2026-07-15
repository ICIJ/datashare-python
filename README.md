# Python workers for Temporal in Datashare

This project serves as a repository of Temporal workers and workflows written in Python
(useful in machine learning) for use with [Datashare](https://icij.gitbook.io/datashare). Install with 

```
make install
```

## Running workers locally (CPU)

### Prerequisites

Install ffmpeg (required by torchcodec):

```bash
sudo apt-get install -y ffmpeg
```

Install Python dependencies for the ASR worker (from `workers/asr-worker/`):

```bash
cd workers/asr-worker
uv sync --extra preprocessing --extra inference --extra cpu
```

Install the workflow worker (from `workers/workflows-worker/`):

```bash
cd workers/workflows-worker
uv sync
```

After installing the workflow worker, patch `ASRArgs` to accept extra fields sent by Datashare (e.g. `user`, `languages`):

```bash
# In workers/workflows-worker/.venv/lib/python3.12/site-packages/asr_worker/objects.py
# Add to the ASRArgs class:
#   model_config = ConfigDict(extra="ignore")
```

### Environment variables

All workers share the same environment variables:

```bash
export DS_WORKER_TEMPORAL__HOST=temporal:7233
export DS_WORKER_TEMPORAL__NAMESPACE=datashare-default
export DS_WORKER_ELASTICSEARCH__ADDRESS=http://elasticsearch:9200
export DS_WORKER_DOCS_ROOT=/
export DS_WORKER_ARTIFACTS_ROOT=/home/dev/src/datashare/artifacts
export DS_WORKER_WORKDIR=/home/dev/workdir
export DS_WORKER_LOGGING__FORMAT=default
```

### Launching the 4 workers

Each worker must be launched in a separate terminal.

**1. IO worker** (searches audio documents in ES) — from `workers/asr-worker/`:

```bash
uv run datashare-python worker start \
    --dependencies asr.io \
    --queue asr.io \
    --activity asr.transcription.search-audios
```

**2. CPU worker** (preprocessing + postprocessing) — from `workers/asr-worker/`:

```bash
uv run datashare-python worker start \
    --dependencies asr.cpu \
    --queue asr.cpu \
    --activity asr.transcription.preprocess \
    --activity asr.transcription.postprocess
```

**3. Inference worker** (runs the Parakeet model on CPU) — from `workers/asr-worker/`:

```bash
uv run datashare-python worker start \
    --dependencies asr.inference \
    --queue asr.inference.cpu \
    --activity asr.transcription.infer
```

**4. Workflow worker** (orchestrates the pipeline) — from `workers/workflows-worker/`:

```bash
uv run datashare-python worker start \
    --skip-config-discovery \
    --queue Python \
    --workflow asr.transcription
```
