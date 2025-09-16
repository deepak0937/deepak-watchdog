import os
import logging
import time
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("deepak-watchdog")

# -----------------------------
# Configuration
# -----------------------------
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "mySuperSecretToken987654321")
PORT = int(os.environ.get("PORT", 5000))

# -----------------------------
# App init
# -----------------------------
app = FastAPI(
    title="Deepak Watchdog",
    version="1.0",
    description="Service to run Deepak Forecast jobs on demand.",
)

# -----------------------------
# Models
# -----------------------------
class RunNowPayload(BaseModel):
    note: Optional[str] = None
    force: Optional[bool] = False

class RunLogEntry(BaseModel):
    timestamp: str
    note: Optional[str]
    force: bool
    status: str
    duration_seconds: float
    error: Optional[str] = None

# -----------------------------
# State
# -----------------------------
RUN_LOG: List[Dict[str, Any]] = []

# -----------------------------
# Helpers
# -----------------------------
def _extract_token(request: Request) -> Optional[str]:
    """Extract admin token from headers, bearer, or query param."""
    token = request.headers.get("x-admin-token")
    if token:
        return token
    auth = request.headers.get("authorization", "")
    if auth and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    qp = request.query_params.get("admin_token")
    if qp:
        return qp
    return None

def do_work(note: Optional[str], force: bool):
    """Simulated background job. Replace with Deepak Forecast logic."""
    start_ts = time.time()
    logger.info("do_work START - note=%s force=%s", note, force)
    entry: Dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": note,
        "force": force,
        "status": "ok",
        "duration_seconds": 0.0,
    }
    try:
        # Simulate actual job
        time.sleep(2)
        entry["duration_seconds"] = round(time.time() - start_ts, 2)
        logger.info("do_work DONE: %s", entry)
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = str(e)
        entry["duration_seconds"] = round(time.time() - start_ts, 2)
        logger.exception("do_work ERROR: %s", e)
    finally:
        RUN_LOG.append(entry)

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
async def root():
    return {"message": "Deepak Watchdog running", "port": PORT}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/status")
async def status():
    """Return last 20 job runs."""
    return {"status": "ok", "recent_runs": RUN_LOG[-20:]}

@app.post("/run-now")
async def run_now(payload: RunNowPayload, background_tasks: BackgroundTasks, request: Request):
    token = _extract_token(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(do_work, payload.note, bool(payload.force))
    logger.info("Accepted run-now job: note=%s force=%s", payload.note, payload.force)
    return {"accepted": True, "note": payload.note, "force": payload.force}

@app.post("/shutdown")
async def shutdown(request: Request):
    token = _extract_token(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    logger.warning("Shutdown requested by admin.")
    return {"shutdown": "requested"}

# -----------------------------
# Lifecycle
# -----------------------------
@app.on_event("startup")
async def on_startup():
    logger.info("Deepak Watchdog starting on port %s", PORT)
    logger.info("ADMIN_TOKEN is set: %s", "yes" if ADMIN_TOKEN else "no")

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Deepak Watchdog shutting down gracefully.")

# -----------------------------
# Local dev entrypoint
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Running locally with uvicorn on 0.0.0.0:%s", PORT)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
