# syntax=docker/dockerfile:1
FROM python:3.11-slim-bullseye AS worker-base
ENV HOME=/home/user
ENV ICIJ_WORKER_TYPE=amqp

RUN apt-get update && apt-get install -y build-essential curl
RUN curl -LsSf https://astral.sh/uv/0.5.6/install.sh | sh
ENV PATH="$HOME/.local/bin:$PATH"
ENV UV_LINK_MODE=copy

WORKDIR $HOME/src/app
ADD scripts  ./scripts/
ADD ml_worker/  ./ml_worker/
ADD uv.lock pyproject.toml README.md ./

FROM worker-base AS worker
ARG n_workers
ENV N_PROCESSING_WORKERS=$n_workers
RUN --mount=type=cache,target=~/.cache/uv uv sync --frozen --compile-bytecode
RUN rm -rf ~/.cache/pip ~/.cache/uv
ENTRYPOINT ["/home/user/src/app/scripts/worker_entrypoint.sh"]
