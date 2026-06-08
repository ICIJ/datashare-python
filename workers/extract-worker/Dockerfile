FROM python:3.13-slim-trixie AS builder


FROM python:3.13-slim-trixie AS extract-worker-builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

WORKDIR /app


FROM extract-worker-builder AS io-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM extract-worker-builder AS extract-cpu-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra base --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_cpu_worker.sh ./entrypoints/extract_cpu_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra base --extra cpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/extract_cpu_worker.sh"]


FROM extract-worker-builder AS extract-gpu-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra base --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_gpu_worker.sh ./entrypoints/extract_gpu_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra base --extra gpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/extract_gpu_worker.sh"]


FROM extract-worker-builder AS extract-cpu-mineru-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_cpu_worker.sh ./entrypoints/extract_cpu_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra cpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/extract_cpu_worker.sh"]


FROM extract-worker-builder AS extract-gpu-mineru-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_gpu_worker.sh ./entrypoints/extract_gpu_worker.sh

# Then install service
RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra gpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/extract_gpu_worker.sh"]
