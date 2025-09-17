# Dockerfile - for Render / Production
# Pin Python to a specific patch release to match CI & avoid ABI mismatch
FROM python:3.11.12-slim

# Prevent python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy requirements first (cache friendly)
COPY requirements.txt .

# Install build deps, install python deps, then remove build deps to keep image small
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential gcc \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && apt-get purge -y --auto-remove build-essential gcc \
 && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# Optional default port (Render will override $PORT at runtime)
ENV PORT=10000
EXPOSE 10000

# ---------- Start command ----------
# Use the shell form so $PORT is expanded at container runtime.
# This assumes your app instance is named `app` inside deepak_watchdog.py
# (FastAPI recommended: use the Uvicorn worker).
CMD ["/bin/sh", "-lc", "gunicorn deepak_watchdog:app --bind 0.0.0.0:$PORT -k uvicorn.workers.UvicornWorker --workers 3 --timeout 120"]
