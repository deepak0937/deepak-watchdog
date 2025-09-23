# Dockerfile - for Render / Production
FROM python:3.11.12-slim

# Prevent python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy requirements first (cache friendly)
COPY requirements.txt .

# Install build deps and Python deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential gcc make \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# Optional default port (Render will override $PORT at runtime)
ENV PORT=10000
EXPOSE 10000

# ---------- Start command ----------
# Use gunicorn with uvicorn workers for FastAPI
CMD ["/bin/sh", "-lc", "gunicorn deepak_watchdog:app --bind 0.0.0.0:$PORT -k uvicorn.workers.UvicornWorker --workers 3 --timeout 120"]
