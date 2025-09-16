FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# shell form so $PORT is expanded at container runtime by the shell
CMD uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers
# If you prefer gunicorn in Docker, replace the above CMD with:
# CMD gunicorn -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT --log-level info --access-logfile -
