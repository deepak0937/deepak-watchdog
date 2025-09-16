FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# IMPORTANT: use deepak_watchdog:app since file is deepak_watchdog.py
CMD uvicorn deepak_watchdog:app --host 0.0.0.0 --port $PORT --proxy-headers
