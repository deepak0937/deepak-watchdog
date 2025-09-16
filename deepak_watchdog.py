# deepak_watchdog.py
"""
Deepak Watchdog - Full single-file app
Features:
- Fetch Groww live quote and historical candle endpoints
- Call OpenAI Chat Completions for a JSON forecast
- Local deterministic fallback forecast
- Persist runs to Postgres (if DATABASE_URL set)
- Optionally send Telegram / Slack notifications
- FastAPI endpoints: /health, /status, /run-now, /shutdown
"""

import os
import time
import json
import re
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter, Retry
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

# SQLAlchemy for persistence (optional)
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Text, MetaData, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError

# ---------- CONFIG from env ----------
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "mySuperSecretAdminToken")
PORT = int(os.environ.get("PORT", "10000"))

GROW_BASE_URL = os.environ.get("GROW_BASE_URL", "https://api.groww.in")
GROW_ACCESS_TOKEN = os.environ.get("GROW_ACCESS_TOKEN", "")   # required for Grow calls
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")         # required for ChatGPT calls
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")  # change if you have gpt-4 access

DATABASE_URL = os.environ.get("DATABASE_URL", "")  # optional postgres: postgres://user:pw@host:5432/db
NOTIFY_TELEGRAM_TOKEN = os.environ.get("NOTIFY_TELEGRAM_TOKEN", "")
NOTIFY_TELEGRAM_CHAT_ID = os.environ.get("NOTIFY_TELEGRAM_CHAT_ID", "")
NOTIFY_SLACK_WEBHOOK = os.environ.get("NOTIFY_SLACK_WEBHOOK", "")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("deepak-watchdog")

# ---------- FastAPI ----------
app = FastAPI(title="Deepak Watchdog - Groww + ChatGPT")

# ---------- In-memory cache for quick status ----------
RUN_LOG: List[Dict[str, Any]] = []  # last runs (kept in-memory for /status)

# ---------- DB Schema (SQLAlchemy) ----------
metadata = MetaData()
runs_table = Table(
    "runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", TIMESTAMP, nullable=False),
    Column("note", String(255)),
    Column("force", String(10)),
    Column("status", String(32)),
    Column("duration", Float),
    Column("ltp", Float),
    Column("open_interest", Float),
    Column("iv", Float),
    Column("snapshot_json", Text),
    Column("forecast_text", Text),
    Column("forecast_confidence", Integer),
    Column("forecast_reason", Text),
)

engine = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        metadata.create_all(engine)
        logger.info("DB connected; runs table ensured.")
    except Exception as e:
        logger.exception("DB init failed: %s", e)
        engine = None
else:
    logger.info("DATABASE_URL not set; persistence disabled.")

# ---------- HTTP session with retries ----------
def requests_session(retries: int = 3, backoff: float = 0.6) -> requests.Session:
    s = requests.Session()
    r = Retry(total=retries, backoff_factor=backoff, status_forcelist=[429, 502, 503, 504],
              allowed_methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS"])
    adapter = HTTPAdapter(max_retries=r)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# ---------- Utility: parse JSON block from ChatGPT text ----------
def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """Try to extract JSON object from text."""
    try:
        m = re.search(r"(\{.*\})", text, re.DOTALL)
        json_text = m.group(1) if m else text
        return json.loads(json_text)
    except Exception:
        try:
            # fallback: try single quotes -> double quotes
            normalized = text.replace("'", '"')
            m = re.search(r"(\{.*\})", normalized, re.DOTALL)
            jt = m.group(1) if m else normalized
            return json.loads(jt)
        except Exception:
            logger.debug("Failed to parse JSON from ChatGPT content: %s", text)
            return None

# ---------- ChatGPT helper (OpenAI REST) ----------
def ask_openai_for_forecast(snapshot: Dict[str, Any], model: str = OPENAI_MODEL) -> Optional[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        logger.info("OPENAI_API_KEY not set; skipping ChatGPT.")
        return None

    # Keep prompt compact to reduce cost
    prompt = (
        "You are a short market forecast assistant. Given the numeric snapshot, "
        "return ONLY a JSON object with keys: forecast (short string), confidence (0-100 integer), reason (one-sentence).\n\n"
        f"Snapshot: {snapshot}\n\nOutput JSON only."
    )

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return only JSON object in your final message."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0,
        "max_tokens": 200,
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=20)
        resp.raise_for_status()
        j = resp.json()
        content = j["choices"][0]["message"]["content"].strip()
        parsed = extract_json(content)
        if not parsed:
            logger.warning("OpenAI returned non-JSON content; content: %s", content[:500])
            return None
        # normalize
        forecast = parsed.get("forecast") or parsed.get("prediction") or parsed.get("label")
        confidence = parsed.get("confidence")
        try:
            confidence = int(confidence) if confidence is not None else 0
        except Exception:
            confidence = 0
        reason = parsed.get("reason") or parsed.get("explanation") or ""
        return {"forecast": str(forecast), "confidence": confidence, "reason": str(reason)}
    except Exception as e:
        logger.exception("OpenAI request failed: %s", e)
        return None

# ---------- Groww API helpers ----------
def grow_live_quote(trading_symbol: str = "NIFTY", exchange: str = "NSE", segment: str = "CASH") -> Dict[str, Any]:
    """Call Groww live quote endpoint -> returns payload dict or error."""
    session = requests_session()
    endpoint = f"{GROW_BASE_URL}/v1/live-data/quote"
    headers = {
        "Authorization": f"Bearer {GROW_ACCESS_TOKEN}",
        "Accept": "application/json",
        "X-API-VERSION": "1.0"
    }
    params = {"exchange": exchange, "segment": segment, "trading_symbol": trading_symbol}
    try:
        r = session.get(endpoint, headers=headers, params=params, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.exception("Grow live-quote call failed: %s", e)
        return {"error": str(e)}

def grow_historical_bulk(groww_symbol: str, start_time: str, end_time: str, exchange: str = "NSE", segment: str = "CASH") -> Dict[str, Any]:
    """
    Call historical bulk candle endpoint.
    start_time/end_time are strings like '2025-01-01 09:15:00' per Grow docs.
    """
    session = requests_session()
    endpoint = f"{GROW_BASE_URL}/v1/historical/candle/bulk"
    headers = {"Authorization": f"Bearer {GROW_ACCESS_TOKEN}", "Accept": "application/json", "X-API-VERSION": "1.0"}
    params = {
        "exchange": exchange,
        "segment": segment,
        "groww_symbol": groww_symbol,
        "start_time": start_time,
        "end_time": end_time,
    }
    try:
        r = session.get(endpoint, headers=headers, params=params, timeout=25)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.exception("Grow historical bulk call failed: %s", e)
        return {"error": str(e)}

# ---------- Local deterministic forecast (fallback) ----------
def compute_local_forecast(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Small deterministic rule-based fallback. Return forecast, confidence, reason."""
    try:
        ltp = snapshot.get("last_price") or snapshot.get("last_price_ltp") or snapshot.get("close")
        if ltp is None:
            return {"forecast": "no-data", "confidence": 0, "reason": "missing price"}
        oi = snapshot.get("open_interest") or 0
        iv = snapshot.get("implied_volatility") or 0
        # simple rules
        if iv and iv > 0.40:
            return {"forecast": "volatile", "confidence": 50, "reason": f"high IV {iv}"}
        if oi and oi > 200000:
            return {"forecast": "bullish", "confidence": 60, "reason": f"high OI {oi}"}
        # numeric parity toy rule
        try:
            if int(float(ltp)) % 2 == 0:
                return {"forecast": "slightly-bullish", "confidence": 40, "reason": "ltp parity rule"}
        except Exception:
            pass
        return {"forecast": f"neutral (ltp {ltp})", "confidence": 30, "reason": "default fallback"}
    except Exception as e:
        logger.exception("compute_local_forecast error: %s", e)
        return {"forecast": "error", "confidence": 0, "reason": str(e)}

# ---------- Persistence ----------
def persist_to_db(entry: Dict[str, Any]):
    if not engine:
        logger.debug("No DB engine available; skipping persistence")
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                runs_table.insert().values(
                    ts=entry["timestamp"],
                    note=entry.get("note"),
                    force=str(entry.get("force")),
                    status=entry.get("status"),
                    duration=entry.get("duration_seconds"),
                    ltp=entry.get("ltp"),
                    open_interest=entry.get("open_interest"),
                    iv=entry.get("iv"),
                    snapshot_json=entry.get("snapshot_json"),
                    forecast_text=entry.get("forecast_text"),
                    forecast_confidence=entry.get("forecast_confidence"),
                    forecast_reason=entry.get("forecast_reason"),
                )
            )
        logger.info("Persisted run to DB")
    except SQLAlchemyError as e:
        logger.exception("DB persist error: %s", e)

# ---------- Notifications (optional) ----------
def notify_telegram(text: str):
    if not (NOTIFY_TELEGRAM_TOKEN and NOTIFY_TELEGRAM_CHAT_ID):
        return
    try:
        url = f"https://api.telegram.org/bot{NOTIFY_TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": NOTIFY_TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception:
        logger.exception("Telegram notify failed")

def notify_slack(text: str):
    if not NOTIFY_SLACK_WEBHOOK:
        return
    try:
        requests.post(NOTIFY_SLACK_WEBHOOK, json={"text": text}, timeout=10)
    except Exception:
        logger.exception("Slack notify failed")

# ---------- Core worker ----------
def do_work(note: Optional[str] = None, force: bool = False, trading_symbol: str = "NIFTY"):
    start_ts = time.time()
    logger.info("do_work START note=%s force=%s symbol=%s", note, force, trading_symbol)

    entry = {
        "timestamp": datetime.utcnow(),
        "note": note,
        "force": force,
        "status": "error",
        "duration_seconds": 0.0,
        "ltp": None,
        "open_interest": None,
        "iv": None,
        "snapshot_json": None,
        "forecast_text": None,
        "forecast_confidence": None,
        "forecast_reason": None,
    }

    if not GROW_ACCESS_TOKEN:
        entry["error"] = "Missing GROW_ACCESS_TOKEN"
        RUN_LOG.append(entry)
        logger.error(entry["error"])
        return entry

    # 1) Fetch live snapshot
    live = grow_live_quote(trading_symbol=trading_symbol)
    payload = live.get("payload") if isinstance(live, dict) else None

    # extract common fields safely
    ltp = None
    oi = None
    iv = None
    if payload and isinstance(payload, dict):
        # Grow docs show fields like last_price, open_interest, implied_volatility
        ltp = payload.get("last_price") or payload.get("lastPrice") or payload.get("last_trade_price") or payload.get("last_price_ltp")
        oi = payload.get("open_interest") or payload.get("openInterest")
        iv = payload.get("implied_volatility") or payload.get("impliedVolatility") or payload.get("iv")

    entry["ltp"] = float(ltp) if ltp is not None else None
    entry["open_interest"] = float(oi) if oi is not None else None
    entry["iv"] = float(iv) if iv is not None else None
    entry["snapshot_json"] = json.dumps(live)[:20000] if live else None
    entry["status"] = "ok"
    entry["duration_seconds"] = round(time.time() - start_ts, 2)

    # small snapshot for reasoning
    snapshot_short = {"ltp": entry["ltp"], "open_interest": entry["open_interest"], "iv": entry["iv"]}

    # 2) Ask OpenAI (ChatGPT) for analysis
    ai_res = ask_openai_for_forecast(snapshot_short)
    if ai_res and ai_res.get("forecast"):
        entry["forecast_text"] = ai_res.get("forecast")
        entry["forecast_confidence"] = int(ai_res.get("confidence", 0))
        entry["forecast_reason"] = ai_res.get("reason", "")
        logger.info("AI forecast: %s (conf=%s)", entry["forecast_text"], entry["forecast_confidence"])
    else:
        # 3) fallback local forecast
        fallback = compute_local_forecast(snapshot_short)
        entry["forecast_text"] = fallback["forecast"]
        entry["forecast_confidence"] = fallback["confidence"]
        entry["forecast_reason"] = fallback["reason"]
        logger.info("Local fallback forecast: %s (conf=%s)", entry["forecast_text"], entry["forecast_confidence"])

    # 4) persist & notify
    RUN_LOG.append(entry)
    persist_to_db(entry)

    # small notification
    try:
        notif = f"Run {entry['timestamp']} symbol={trading_symbol} forecast={entry['forecast_text']} conf={entry['forecast_confidence']}"
        notify_telegram(notif)
        notify_slack(notif)
    except Exception:
        logger.exception("Notification failed")

    return entry

# ---------- Auth helper ----------
def _extract_admin_token(req: Request) -> Optional[str]:
    # check header first
    auth = req.headers.get("authorization", "")
    if auth:
        if auth.lower().startswith("bearer "):
            return auth.split(" ",1)[1].strip()
        return auth.strip()
    # fallback to x-admin-token
    xt = req.headers.get("x-admin-token")
    if xt:
        return xt.strip()
    # query param fallback
    qt = req.query_params.get("admin_token")
    return qt

# ---------- Routes ----------
@app.get("/")
async def root():
    return {"message": "Deepak Watchdog running", "time": datetime.utcnow().isoformat()}

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

@app.get("/status")
async def status():
    # return last N runs + DB present flag
    return {"status": "ok", "recent_runs": RUN_LOG[-20:], "persistence": bool(engine)}

@app.post("/run-now")
async def run_now(request: Request, background_tasks: BackgroundTasks):
    token = _extract_admin_token(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    body = await request.json() if request.headers.get("content-type","").startswith("application/json") else {}
    note = body.get("note") if isinstance(body, dict) else None
    force = bool(body.get("force")) if isinstance(body, dict) else False
    symbol = body.get("symbol") if isinstance(body, dict) else "NIFTY"
    background_tasks.add_task(do_work, note, force, symbol)
    logger.info("Scheduled background run (note=%s force=%s symbol=%s)", note, force, symbol)
    return JSONResponse({"accepted": True, "note": note, "force": force, "symbol": symbol})

@app.post("/shutdown")
async def shutdown(request: Request):
    token = _extract_admin_token(request)
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    logger.warning("Shutdown requested by admin token")
    # graceful exit
    os.kill(os.getpid(), 15)
    return {"shutdown": "requested"}

# ---------- Startup / Shutdown ----------
@app.on_event("startup")
async def on_startup():
    logger.info("Deepak Watchdog starting; ADMIN_TOKEN set: %s; DB enabled: %s; OPENAI set: %s",
                bool(ADMIN_TOKEN), bool(engine), bool(OPENAI_API_KEY))

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Deepak Watchdog shutting down")

# ---------- Local run entry ----------
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting uvicorn on 0.0.0.0:%s", PORT)
    uvicorn.run("deepak_watchdog:app", host="0.0.0.0", port=PORT, log_level="info")
    # ---------- quick test endpoint for /groww/quote ----------
from fastapi import Query

@app.get("/groww/quote")
async def groww_quote(
    exchange: str = Query(..., description="exchange (NSE)"),
    segment: str = Query(..., description="segment (CASH)"),
    trading_symbol: str = Query(..., description="trading symbol like NIFTY")
):
    """Test stub: echoes back received query params so we can confirm routing."""
    return {
        "ok": True,
        "msg": "groww quote endpoint (test stub)",
        "exchange": exchange,
        "segment": segment,
        "trading_symbol": trading_symbol
    }
# ---------- end test endpoint ----------

