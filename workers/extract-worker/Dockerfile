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

# Flash attention libs has to be built on the hardware and is not available as a prebuild wheel. We don't want to do
# it in the final image since it requires nvcc which is already embedded in Python deps and is super heavy. Sadly the
# embedded version of nvcc is not usable for building
FROM nvidia/cuda:12.9.0-devel-ubuntu24.04 AS flash-attn-builder

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /bin/

RUN uv python install 3.14

# Install torch from the lockfile by install extras and skipping some packages
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-editable --extra base --extra gpu \
    --no-install-package flash-attn \
    --no-install-package torchvision \
    --no-install-package cuda-bindings

RUN --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv export --frozen --extra base-gpu --no-dev | \
    sed -n 's/^flash-attn==\([^; ]*\).*/\1/p'  > flash-attn-version.txt

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install pip && \
    MAX_JOBS=8 uv run python -m pip wheel "flash-attn==$(cat flash-attn-version.txt)" \
    --no-build-isolation --no-deps -w /flash-attn-wheel


FROM runtime-base AS io-worker
ADD io-worker-deps-exclusion.txt ./
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project $(sed 's/^/--no-install-package /' io-worker-deps-exclusion.txt)

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable $(sed 's/^/--no-install-package /' io-worker-deps-exclusion.txt)
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM runtime-base AS extract-base-builder

# TODO: do we really need tesseract here ?
RUN apt update && \
    apt install -y --no-install-recommends build-essential g++ && \
    apt install -y tesseract-ocr && \
    rm -rf /var/lib/apt/lists/*


FROM extract-base-builder AS extract-cpu-build
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

COPY --from=extract-cpu-build /app /app
ENTRYPOINT ["entrypoints/extract_cpu_worker.sh"]


FROM extract-base-builder AS extract-gpu-build
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
COPY --from=extract-gpu-build /app /app

ENTRYPOINT ["entrypoints/extract_gpu_worker.sh"]


FROM runtime-base AS extract-cpu-mineru-build
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra cpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_cpu_worker.sh ./entrypoints/extract_cpu_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra cpu
RUN rm -rf ~/.cache/pip

# Slim runtime (as extract-cpu-worker): no toolchain, keep tesseract
FROM runtime-base AS extract-cpu-mineru-worker

COPY --from=extract-cpu-mineru-build /app /app
ENTRYPOINT ["entrypoints/extract_cpu_mineru_worker.sh"]


FROM runtime-base AS extract-gpu-mineru-build
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra mineru --extra gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD extract_worker ./extract_worker/
ADD entrypoints/extract_gpu_worker.sh ./entrypoints/extract_gpu_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --no-editable --extra mineru --extra gpu
RUN rm -rf ~/.cache/pip

# Slim runtime (as extract-cpu-worker): no toolchain, keep tesseract
FROM runtime-base AS extract-gpu-mineru-worker

COPY --from=extract-gpu-mineru-build /app /app
ENTRYPOINT ["entrypoints/extract_gpu_mineru_worker.sh"]
