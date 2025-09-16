# Dockerfile - explicit gunicorn + uvicorn worker using deepak_watchdog:app
FROM python:3.11-slim

WORKDIR /app

# Install build deps (if packages need wheels) - keep minimal to speed builds
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Unbuffered logs
ENV PYTHONUNBUFFERED=1

# Use gunicorn with UvicornWorker and point to deepak_watchdog:app
# This is robust on Render (if Render runs this image directly).
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "deepak_watchdog:app", "--bind", "0.0.0.0:$PORT", "--log-level", "info", "--access-logfile", "-"]
