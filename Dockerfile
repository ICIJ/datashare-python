# syntax=docker/dockerfile:1

############################
# Base builder
############################

FROM --platform=linux/amd64 python:3.13-slim-trixie AS dp_builder

ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# add task cli
ADD datashare-python/ ./datashare-python/

# install python deps
RUN --mount=type=cache,target=~/.cache/uv \
    uv pip install --system datashare-python/

# copy build-independant files
ADD scripts scripts

# clean up
RUN rm -rf ~/.cache/uv

############################
# ASR
############################
FROM --platform=linux/amd64 python:3.13-slim-trixie AS asr_builder

# args/envs
ARG PREDOWNLOAD_MODELS=true
ARG NEMO_MODEL=nvidia/parakeet-tdt-0.6b-v3

ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy

# install builder deps
RUN apt update && \
    apt install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# copy uv
COPY --from=dp_builder /bin/uv /bin/uvx /bin/

WORKDIR /app

# add asr-worker
ADD asr-worker/ ./asr-worker/
ADD requirements_overrides.txt ./requirements_overrides.txt

# install python deps
RUN --mount=type=cache,target=~/.cache/uv \
    uv pip install --override requirements_overrides.txt --system asr-worker/

# predownload models
RUN if [ "$PREDOWNLOAD_MODELS" = "true" ]; then \
    uv run python -c "from nemo.collections.asr.models import ASRModel; ASRModel.from_pretrained('${NEMO_MODEL}')"; \
    else \
    echo "Models not downloaded; this saves on container space by increasing container startup time."; \
    fi

# clean up
RUN rm -rf ~/.cache/uv

############################
# Runtime stage
############################
FROM --platform=linux/amd64 python:3.13-slim-trixie AS dp_runtime

# args/envs
ARG TEMPORAL_HOST=temporal
ARG TEMPORAL_PORT=7233
ARG WORKERS='["asr_worker.worker.run_asr_worker"]'

ENV PATH="/usr/local/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV TEMPORAL_HOST=${TEMPORAL_HOST}
ENV TEMPORAL_PORT=${TEMPORAL_PORT}
ENV WORKERS=${WORKERS}

# install runtime deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# copy builder files
# create /usr/local first
COPY --from=dp_builder /usr/local/ /usr/local/
COPY --from=dp_builder /app/ /app/
COPY --from=asr_builder /usr/local/ /usr/local/
COPY --from=asr_builder /app/ /app/
COPY --from=asr_builder /root/.cache/huggingface* /root/.cache/huggingface

# clean up
RUN apt autoremove --purge -y && \
    apt clean

ENTRYPOINT /app/scripts/worker_entrypoint.sh ${TEMPORAL_HOST} ${TEMPORAL_PORT} ${WORKERS}
