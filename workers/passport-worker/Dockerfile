# Slim runtime base: debian + uv + the uv-managed interpreter, but not build toolchain.
# The venv built in the *-build stages references the managed interpreter using absolute
# path (see .venv/bin/python -> /root/.local/share/uv/python/...), so we copy it here at
# the same location. Workers are build by copying their /app (venv + code) on top.
FROM debian:trixie-slim AS runtime-base

ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.24 /uv /uvx /bin/

WORKDIR /app

FROM runtime-base AS passport-base-builder

# TODO: do we really need tesseract here ?
RUN apt update && \
    apt install -y --no-install-recommends build-essential g++ && \
    apt install -y tesseract-ocr && \
    rm -rf /var/lib/apt/lists/*

FROM passport-base-builder AS passport-io-worker-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra preprocessing

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD passport_worker ./passport_worker/
ADD entrypoints/passport_io_worker.sh ./entrypoints/passport_io_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --extra preprocessing


FROM runtime-base AS passport-io-worker

COPY --from=passport-io-worker-builder /app /app
COPY --from=passport-io-worker-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/passport_io_worker.sh"]

FROM runtime-base AS extract-base-builder

RUN apt update && \
    apt install -y --no-install-recommends build-essential g++ && \
    rm -rf /var/lib/apt/lists/*


FROM extract-base-builder AS extract-cpu-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra base --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_cpu_worker.sh ./entrypoints/extract_cpu_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra base --extra cpu
RUN rm -rf ~/.cache/pip

FROM runtime-base AS extract-cpu-worker

COPY --from=extract-cpu-builder /app /app
COPY --from=extract-cpu-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/extract_cpu_worker.sh"]


FROM extract-base-builder AS extract-gpu-builder
## Copy the flash-attn wheel
#COPY --from=flash-attn-builder /flash-attn-wheel /flash-attn-wheel
## Install the wheel
#RUN uv pip install /flash-attn-wheel/*.whl --no-deps

# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra base --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_gpu_worker.sh ./entrypoints/extract_gpu_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra base --extra gpu
RUN rm -rf ~/.cache/pip

# Slim runtime (as extract-cpu-worker): no toolchain, keep tesseract
FROM runtime-base AS extract-gpu-worker
RUN apt update && \
    apt install -y --no-install-recommends tesseract-ocr && \
    rm -rf /var/lib/apt/lists/*
COPY --from=extract-gpu-builder /app /app
COPY --from=extract-gpu-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/extract_gpu_worker.sh"]


FROM runtime-base AS extract-cpu-mineru-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_cpu_mineru_worker.sh ./entrypoints/extract_cpu_mineru_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra cpu
RUN rm -rf ~/.cache/pip

# Slim runtime (as extract-cpu-worker): no toolchain, keep tesseract
FROM runtime-base AS extract-cpu-mineru-worker

COPY --from=extract-cpu-mineru-builder /app /app
COPY --from=extract-cpu-mineru-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/extract_cpu_mineru_worker.sh"]


FROM runtime-base AS extract-gpu-mineru-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_gpu_mineru_worker.sh ./entrypoints/extract_gpu_mineru_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra gpu
RUN rm -rf ~/.cache/pip

# Slim runtime (as extract-cpu-worker): no toolchain, keep tesseract
FROM runtime-base AS extract-gpu-mineru-worker

COPY --from=extract-gpu-mineru-builder /app /app
COPY --from=extract-gpu-mineru-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/extract_gpu_mineru_worker.sh"]
