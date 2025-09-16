import os
import logging
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("deepak-watchdog")

# ---------- Config ----------
# Set ADMIN_TOKEN in Render environment variables (do NOT commit secrets)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "mySuperSecretToken987654321")
# Use the PORT Render provides (fallback to 5000 locally)
PORT = int(os.environ.get("PORT", 5000))

# ---------- FastAPI app ----------
app = FastAPI(title="deepak-watchdog", version="1.0")

# ---------- Models ----------
class RunNowPayload(BaseModel):
    note: Optional[str] = None
    force: Optional[bool] = False

# ---------- In-memory run log (demo) ----------
RUN_LOG = []


# ---------- Worker logic ----------
def do_work(note: Optional[str], force: bool):
    """
    Replace or extend this function with your actual watchdog logic.
    This runs in the background (called by BackgroundTasks).
    """
    start_ts = time.time()
    logger.info("do_work START - note=%s force=%s", note, force)

    try:
        # ---- placeholder work (replace with real job) ----
        # Example: gather market cues, compute Deepak Trend, write to DB, etc.
        time.sleep(2)

        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "note": note,
            "force": bool(force),
            "status": "ok",
            "duration_seconds": round(time.time() - start_ts, 2),
        }
        RUN_LOG.append(entry)
        logger.info("do_work OK: %s", entry)
    except Exception as e:
        logger.exception("do_work ERROR: %s", e)
        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "note": note,
            "force": bool(force),
            "status": "error",
            "error": str(e),
            "duration_seconds": round(time.time() - start_ts, 2),
        }
        RUN_LOG.append(entry)


# ---------- Helper: auth ----------
def _extract_token_from_request(request: Request) -> Optional[str]:
    # Preferred header
    token = request.headers.get("x-admin-token")
    if token:
        return token

    # Authorization: Bearer <token>
    auth = request.headers.get("authorization", "")
    if auth and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()

    # Query param
    qp = request.query_params.get("admin_token")
    if qp:
        return qp

    return None


# ---------- Routes ----------
@app.get("/")
async def root():
    return {"message": "deepak-watchdog running", "port": PORT}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    # Provide a lightweight status including last 10 runs
    return {"status": "ok", "recent_runs": RUN_LOG[-10:]}


@app.post("/run-now")
async def run_now(payload: RunNowPayload, background_tasks: BackgroundTasks, request: Request):
    """
    Trigger the watchdog. Protected by ADMIN_TOKEN.
    Accepts:
      - X-ADMIN-TOKEN header
      - Authorization: Bearer <token>
      - ?admin_token=...
    """
    token = _extract_token_from_request(request)
    if token != ADMIN_TOKEN:
        client = request.client.host if request.client else "unknown"
        logger.warning("Unauthorized /run-now attempt from %s", client)
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Schedule background job and return immediately
    background_tasks.add_task(do_work, payload.note, bool(payload.force))
    logger.info("Accepted /run-now (background job scheduled) note=%s force=%s", payload.note, payload.force)
    return JSONResponse({"accepted": True, "note": payload.note, "force": payload.force})


@app.post("/shutdown")
async def shutdown(request: Request):
    """
    Admin-only request to request shutdown (note: Render controls lifecycle).
    This endpoint merely logs the request and returns confirmation.
    """
    token = _extract_token_from_request(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    logger.warning("Admin requested shutdown")
    return {"shutdown": "requested"}


# ---------- Startup / Shutdown logging ----------
@app.on_event("startup")
async def on_startup():
    logger.info("deepak-watchdog starting up. Listening on port %s", PORT)
    logger.info("ADMIN_TOKEN set: %s", "yes" if ADMIN_TOKEN else "no")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("deepak-watchdog shutting down gracefully")


# ---------- If run directly (local dev) ----------
if __name__ == "__main__":
    import uvicorn

    logger.info("Starting uvicorn directly on 0.0.0.0:%s (development mode)", PORT)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
