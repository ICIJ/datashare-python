# Slim runtime base: debian + uv + the uv-managed interpreter, but not build toolchain.
# The venv built in the *-build stages references the managed interpreter using absolute
# path (see .venv/bin/python -> /root/.local/share/uv/python/...), so we copy it here at
# the same location. Workers are build by copying their /app (venv + code) on top.
FROM debian:bookworm-slim AS runtime-base

ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.24 /uv /uvx /bin/

WORKDIR /app

FROM runtime-base AS passport-base-builder
ENV PATH="/root/.local/bin:$PATH"
RUN uv python install 3.12

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


FROM passport-base-builder AS passport-preprocessing-worker-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra preprocessing

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD passport_worker ./passport_worker/
ADD entrypoints/passport_preprocessing_worker.sh ./entrypoints/passport_preprocessing_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --extra preprocessing


FROM runtime-base AS passport-preprocessing-worker

COPY --from=passport-preprocessing-worker-builder /app /app
COPY --from=passport-preprocessing-worker-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/passport_preprocessing_worker.sh"]

FROM passport-base-builder AS passport-inference-worker-builder
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra inference-gpu

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD passport_worker ./passport_worker/
ADD entrypoints/passport_inference_worker.sh ./entrypoints/passport_inference_worker.sh

# Then install service
RUN --mount=type=cache,target=/root/.cache/uv uv sync -v --frozen --extra inference-gpu


FROM runtime-base AS passport-inference-worker
RUN cat /etc/apt/sources.list.d/*
RUN apt update && apt install -y \
    ffmpeg  \
    libsm6  \
    libxext6 \
    tesseract-ocr \
    wget

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
RUN wget https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/cuda-keyring_1.1-1_all.deb \
    && dpkg -i cuda-keyring_1.1-1_all.deb \
    && apt update
RUN apt -y install \
    cuda-toolkit-13-3 \
    cudnn9-cuda-13

COPY --from=passport-inference-worker-builder /app /app
COPY --from=passport-inference-worker-builder /root/.local/share/uv/python /root/.local/share/uv/python
ENTRYPOINT ["entrypoints/passport_inference_worker.sh"]
