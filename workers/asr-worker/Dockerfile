FROM python:3.13-slim-trixie AS builder

RUN apt update && \
    apt install -y --no-install-recommends build-essential g++ && \
    rm -rf /var/lib/apt/lists/*


FROM python:3.13-slim-trixie AS asr-worker-builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

RUN apt-get update && \
    apt-get install -y ffmpeg libavcodec-dev libavformat-dev libavutil-dev

WORKDIR /app


FROM asr-worker-builder AS io-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD asr_worker ./asr_worker/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service

RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra cpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM asr-worker-builder AS preprocessing-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra preprocessing --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD asr_worker ./asr_worker/
ADD entrypoints/preprocessing_worker.sh ./entrypoints/preprocessing_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra preprocessing --extra cpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/preprocessing_worker.sh"]


FROM asr-worker-builder AS inference-gpu-worker
RUN apt-get update && apt-get install -y build-essential
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra inference --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD asr_worker ./asr_worker/
ADD entrypoints/inference_gpu_worker.sh ./entrypoints/inference_gpu_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra inference
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/inference_gpu_worker.sh"]
