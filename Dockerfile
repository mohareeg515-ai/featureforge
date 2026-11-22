# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 UV_LINK_MODE=copy
RUN pip install --no-cache-dir uv==0.11.32
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable
ENV PATH="/app/.venv/bin:$PATH" FF_DATA_ROOT=/tmp/featureforge
RUN useradd --create-home --uid 10001 featureforge && mkdir -p /tmp/featureforge \
    && chown featureforge /tmp/featureforge
USER featureforge
# One logical batch per container run; the scheduler passes `--date`.
ENTRYPOINT ["featureforge"]
CMD ["run"]
