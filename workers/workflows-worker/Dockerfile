FROM python:3.13-slim-trixie AS worker-builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

WORKDIR /app


FROM worker-builder AS datashare-workflow-worker
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=datashare-python/uv.dist.lock,target=uv.lock \
    --mount=type=bind,source=datashare-python/pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-install-project

# Then copy code
ADD datashare-python/uv.dist.lock ./uv.lock
ADD pyproject.toml README.md ./
ADD asr_worker ./asr_worker/
ADD entrypoints/io_worker.sh ./entrypoints/io_worker.sh

# Then install service

RUN --mount=type=cache,target=~/.cache/uv uv sync -v --frozen --no-editable --extra cpu
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["scripts/all_workflows_worker_entrypoint.sh"]
