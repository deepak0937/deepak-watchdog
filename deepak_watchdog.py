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
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "mySuperSecretToken987654321")
PORT = int(os.environ.get("PORT", 5000))

# ---------- App ----------
app = FastAPI(title="deepak-watchdog", version="1.0")

class RunNowPayload(BaseModel):
    note: Optional[str] = None
    force: Optional[bool] = False

RUN_LOG = []

# ---------- Worker ----------
def do_work(note: Optional[str], force: bool):
    start_ts = time.time()
    logger.info("do_work START - note=%s force=%s", note, force)
    time.sleep(2)  # simulate job
    entry = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": note,
        "force": bool(force),
        "status": "ok",
        "duration_seconds": round(time.time() - start_ts, 2),
    }
    RUN_LOG.append(entry)
    logger.info("do_work DONE: %s", entry)

def _extract_token(request: Request) -> Optional[str]:
    token = request.headers.get("x-admin-token")
    if token: return token
    auth = request.headers.get("authorization", "")
    if auth and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    qp = request.query_params.get("admin_token")
    if qp: return qp
    return None

# ---------- Routes ----------
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/status")
async def status():
    return {"recent_runs": RUN_LOG[-10:]}

@app.post("/run-now")
async def run_now(payload: RunNowPayload, background_tasks: BackgroundTasks, request: Request):
    token = _extract_token(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(do_work, payload.note, bool(payload.force))
    return JSONResponse({"accepted": True, "note": payload.note, "force": payload.force})

# ---------- Local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
