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

# Ensure the persistent-disk mount point exists even when no disk is attached
# (e.g. local runs / Render free tier). On paid Render, the disk mounts here and
# shadows this dir; render.yaml overrides ATTRIBUTIONOPS_DB_PATH to
# /var/data/attributionops.sqlite so the DB lives on the persistent disk.
RUN mkdir -p /var/data

# Default DB path for local/standalone runs. render.yaml overrides this to
# /var/data/attributionops.sqlite (the persistent disk) when deployed on Render.
ENV ATTRIBUTIONOPS_DB_PATH=/app/data/live/attributionops.sqlite
ENV REPORT_TIMEZONE=Etc/GMT+6
ENV PYTHONUNBUFFERED=1
ENV TRACKING_DOMAIN=""

EXPOSE 8000

# Worker count is configurable via WEB_CONCURRENCY (default 1). The blocking
# report/DB endpoints run in FastAPI's threadpool, so a single worker already
# serves requests concurrently while keeping the event loop responsive.
# NOTE: the live-feed WebSocket broadcaster keeps connection state per process,
# so scaling beyond 1 worker needs a shared pub/sub (e.g. Redis) first.
CMD uvicorn main:app --host 0.0.0.0 --port 8000 --app-dir /app/backend --workers ${WEB_CONCURRENCY:-1}
