FROM python:3.13-slim-trixie AS builder

RUN apt update && \
    apt install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*


FROM python:3.13-slim-trixie AS worker_template_builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app


FROM worker_template_builder AS io_worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD worker_template ./worker_template/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN cd worker_template && uv sync -v --frozen --no-editable --no-install-project
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/io_worker.sh"]


FROM worker_template_builder AS translate_worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra ml

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD worker_template ./worker_template/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN cd worker_template && uv sync -v --frozen --no-editable --no-install-project --extra ml
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/translate_worker.sh"]


FROM worker_template_builder AS classify_worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project --extra ml

# Then copy code
ADD uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD worker_template ./worker_template/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service
RUN cd worker_template && uv sync -v --frozen --no-editable --no-install-project --extra ml
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["entrypoints/classify_worker.sh"]
