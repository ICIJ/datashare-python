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
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM translation-worker-builder AS inference-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra inference

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD translation_worker ./translation_worker/
ADD entrypoints/inference_worker.sh ./entrypoints/inference_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra inference
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/inference_worker.sh"]
