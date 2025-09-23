# gunicorn.conf.py
bind = "0.0.0.0:$PORT"
worker_class = "uvicorn.workers.UvicornWorker"
workers = 3
timeout = 120
graceful_timeout = 30
max_requests = 1000
max_requests_jitter = 50
capture_output = True
loglevel = "info"
preload_app = False    # safer: don't preload if you open resources at import-time
