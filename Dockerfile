FROM python:3.12-slim

WORKDIR /app

# Copy project files
COPY attributionops/ /app/attributionops/
COPY scripts/ /app/scripts/
COPY backend/ /app/backend/
COPY .env.example /app/.env.example

# Install Python dependencies from the pinned requirements file (reproducible).
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Create empty database with schema only (no dummy data)
RUN mkdir -p /app/data/live && \
    python /app/scripts/init_empty_db.py \
    --sqlite-path /app/data/live/attributionops.sqlite

ENV ATTRIBUTIONOPS_DB_PATH=/app/data/live/attributionops.sqlite
ENV PYTHONUNBUFFERED=1
ENV TRACKING_DOMAIN=""

EXPOSE 8000

# Worker count is configurable via WEB_CONCURRENCY (default 1). The blocking
# report/DB endpoints run in FastAPI's threadpool, so a single worker already
# serves requests concurrently while keeping the event loop responsive.
# NOTE: the live-feed WebSocket broadcaster keeps connection state per process,
# so scaling beyond 1 worker needs a shared pub/sub (e.g. Redis) first.
CMD uvicorn main:app --host 0.0.0.0 --port 8000 --app-dir /app/backend --workers ${WEB_CONCURRENCY:-1}
