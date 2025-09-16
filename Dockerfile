FROM python:3.11-slim

WORKDIR /app

# copy dependency list and install first (cache layer)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY . .

# ensure logs are unbuffered
ENV PYTHONUNBUFFERED=1

# runtime command: use uvicorn and expand $PORT at runtime
CMD uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers
# Alternative (production) you can use instead:
# CMD gunicorn -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT --log-level info --access-logfile -
