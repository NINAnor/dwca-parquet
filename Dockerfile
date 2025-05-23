FROM python:3.12-slim

RUN apt-get update && apt-get install -y git
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

ADD . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

RUN AWS_SECRET_KEY="" AWS_ENDPOINT_URL="" AWS_ACCESS_KEY="" S3_BUCKET="" IPT_PUBLIC="" REDIS_URL="" uv run dwca-setup

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["uv", "run", "uvicorn", "dwca_parquet.main:app"]
