FROM python:3.13-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev
COPY alembic.ini ./
COPY migrations ./migrations
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
CMD ["uvicorn", "payment_service.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
