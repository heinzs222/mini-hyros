FROM python:3.12-slim

WORKDIR /app

# Copy project files
COPY attributionops/ /app/attributionops/
COPY scripts/ /app/scripts/
COPY backend/ /app/backend/
COPY .env.example /app/.env.example

# Install Python dependencies
RUN pip install --no-cache-dir fastapi uvicorn[standard] websockets python-dotenv httpx

# Create empty database with schema only (no dummy data)
RUN mkdir -p /app/data/live && \
    python /app/scripts/init_empty_db.py \
    --sqlite-path /app/data/live/attributionops.sqlite

ENV ATTRIBUTIONOPS_DB_PATH=/app/data/live/attributionops.sqlite
ENV PYTHONUNBUFFERED=1
ENV TRACKING_DOMAIN=""

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/app/backend"]
