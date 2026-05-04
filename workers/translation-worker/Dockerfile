FROM python:3.13-slim-trixie AS builder


FROM python:3.13-slim-trixie AS translation-worker-builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

WORKDIR /app


FROM translation-worker-builder AS io-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD translation_worker ./translation_worker/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN cd translation_worker && uv sync -v --frozen --no-editable
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM translation-worker-builder AS inference-cpu-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra inference --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD translation_worker ./translation_worker/
ADD entrypoints/inference_cpu_worker.sh ./entrypoints/inference_cpu_worker.sh

# Then install service
RUN cd translation_worker && uv sync -v --frozen --no-editable --extra inference --extra cpu
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/inference_cpu_worker.sh"]


FROM translation-worker-builder AS inference-gpu-worker
RUN apt-get update && apt-get install -y build-essential
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra inference --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD translation_worker ./translation_worker/
ADD entrypoints/inference_gpu_worker.sh ./entrypoints/inference_gpu_worker.sh

# Then install service
RUN cd translation_worker && uv sync -v --frozen --no-editable --extra inference --extra gpu
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/inference_gpu_worker.sh"]
