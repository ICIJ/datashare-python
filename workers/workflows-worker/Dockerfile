FROM python:3.13-slim-trixie AS worker-builder

ENV PYTHONUNBUFFERED=1
ENV UV_HTTP_TIMEOUT=300
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_DEV=1

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

RUN apt update && \
    apt install -y --no-install-recommends build-essential g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app


FROM worker-builder AS datashare-workflows-worker

ADD pyproject.toml README.md main.py all_workflows_worker_entrypoint.sh ./
ADD uv.dist.lock ./uv.lock

# Install deps first to optimize layer cache
RUN uv sync -v --frozen --no-editable
RUN rm -rf ~/.cache/pip

ENTRYPOINT ["./all_workflows_worker_entrypoint.sh"]
