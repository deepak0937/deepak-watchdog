# Groww + ChatGPT Watchdog

FastAPI service to fetch Groww data and feed ChatGPT for Nifty options analysis.

## Local quickstart
1. python -m venv venv && source venv/bin/activate
2. pip install -r requirements.txt
3. cp .env.example .env   # fill secrets
4. uvicorn app:app --reload --port 8000

## Branches
- main = production
- stable-api-integration = work in progress
