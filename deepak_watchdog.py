"""
Deepak Watchdog - final updated version
- Robust env handling for Groww keys and tokens
- Try multiple Groww endpoints for quotes
- Safe OpenAI handling (won't crash if OPENAI_KEY missing)
- Validate webhook URL before posting
- Includes /run-now, /latest, /pause, /resume routes
"""

import os
import json
import time
import sqlite3
import requests
from datetime import datetime
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

# ========== CONFIG (via env vars) ==========
DB_PATH = os.getenv("DB_PATH", "deepak_watchdog.db")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # default 5 min
TIMEZONE = os.getenv("TZ", "Asia/Kolkata")

# Groww / token config
GROWW_BASE = os.getenv("GROWW_BASE", "https://api.groww.in")
GROWW_TOKEN_URL = os.getenv("GROWW_TOKEN_URL", f"{GROWW_BASE}/v1/api/token")

# OpenAI config
OPENAI_KEY = os.getenv("OPENAI_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.0"))

# Safety & preferences
MAX_LOSS_RUPEES = int(os.getenv("MAX_LOSS_RUPEES", "11000"))
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "NIFTY").split(",")]

# Admin token for pause/resume/run-now endpoints
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Token refresh frequency (hours)
try:
    GROWW_REFRESH_HOURS = float(os.getenv("GROWW_REFRESH_HOURS", "12"))
except:
    GROWW_REFRESH_HOURS = 12.0

# Telegram/Webhook (optional)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # must start with http:// or https:// if provided

# ========== DATABASE ==========
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
      CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        symbol TEXT,
        market_snapshot TEXT,
        ai_json TEXT,
        ai_raw TEXT
      )
    """)
    c.execute("""
      CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        token TEXT,
        expiry_ts INTEGER
      )
    """)
    conn.commit()
    return conn

DB = init_db()

# ========== NOTIFICATIONS ==========
def telegram_notify(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        if resp.status_code != 200:
            print("[Notify] Telegram returned", resp.status_code, resp.text)
    except Exception as e:
        print("[Notify] telegram error:", e)

def webhook_notify(payload):
    if not WEBHOOK_URL:
        return
    if not (WEBHOOK_URL.startswith("http://") or WEBHOOK_URL.startswith("https://")):
        print("[Notify] webhook error: Invalid URL (no scheme):", WEBHOOK_URL)
        return
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        if resp.status_code >= 300:
            print("[Notify] webhook returned", resp.status_code, resp.text)
    except Exception as e:
        print("[Notify] webhook error:", e)

# ========== TOKEN STORAGE HELPERS ==========
def store_token(token, expires_in=None):
    try:
        expiry_ts = int(time.time()) + int(expires_in) if expires_in else None
    except Exception:
        expiry_ts = None
    cur = DB.cursor()
    cur.execute("DELETE FROM tokens")
    cur.execute("INSERT INTO tokens(id, token, expiry_ts) VALUES (1, ?, ?)", (token, expiry_ts))
    DB.commit()
    print("[Deepak] Stored Groww token. Expires in:", expires_in)

def get_stored_token():
    cur = DB.cursor()
    cur.execute("SELECT token, expiry_ts FROM tokens WHERE id=1")
    r = cur.fetchone()
    if not r:
        return None
    token, expiry_ts = r
    if expiry_ts and int(time.time()) > (expiry_ts - 60):
        print("[Deepak] Stored token expired or near expiry.")
        return None
    return token

# ========== TOKEN REFRESH / ACQUIRE ==========
def refresh_groww_token_if_needed():
    t = get_stored_token()
    if t:
        return t

    env_token = os.getenv("GROWW_TOKEN")
    if env_token:
        try:
            store_token(env_token, None)
            print("[Deepak] Using GROWW_TOKEN from env (stored).")
            return env_token
        except Exception as e:
            print("[Deepak] Failed storing env token:", e)

    key = os.getenv("GROWW_KEY") or os.getenv("GROWW_API_KEY") or os.getenv("GROWW_CLIENT_ID")
    secret = os.getenv("GROWW_SECRET") or os.getenv("GROWW_SECRET_KEY") or os.getenv("GROWW_CLIENT_SECRET")
    token_url = os.getenv("GROWW_TOKEN_URL", GROWW_TOKEN_URL)
    if not key or not secret:
        raise Exception("No stored token and missing GROWW_KEY / GROWW_SECRET to refresh token.")

    payload = {"client_id": key, "client_secret": secret, "grant_type": "client_credentials"}
    try:
        r = requests.post(token_url, json=payload, timeout=12)
        r.raise_for_status()
        j = r.json()
        access_token = (
            j.get("access_token")
            or j.get("accessToken")
            or j.get("token")
            or (j.get("data") or {}).get("token")
            or (j.get("data") or {}).get("access_token")
        )
        expires_in = (
            j.get("expires_in")
            or j.get("expiresIn")
            or (j.get("data") or {}).get("expires_in")
            or (j.get("data") or {}).get("expiry")
        )
        if not access_token:
            raise Exception("Token response missing access token: " + str(j))
        store_token(access_token, expires_in)
        print("[Deepak] Refreshed Groww token successfully. Expires in:", expires_in)
        return access_token
    except Exception as e:
        print("[Deepak] Failed to refresh Groww token:", e)
        if env_token:
            return env_token
        raise

# ========== GROWW DATA FETCH ==========
def fetch_groww_quote(symbol="NIFTY"):
    token = refresh_groww_token_if_needed()
    candidate_paths = [
        "/v1/api/stocks_data/v2/quotes",
        "/v1/stocks_data/quotes",
        "/v1/api/market/quotes",
    ]
    params = {"symbol": symbol}
    last_exc = None
    for path in candidate_paths:
        url = f"{GROWW_BASE}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Client-Id": os.getenv("GROWW_KEY") or os.getenv("GROWW_API_KEY") or ""
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code == 404:
                print(f"[Deepak] fetch_groww_quote: endpoint {path} returned 404, trying next...")
                last_exc = Exception(f"404 for {url}")
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as he:
            status = getattr(he.response, "status_code", None)
            print(f"[Deepak] fetch_groww_quote HTTPError for {url}: {status} {he}")
            last_exc = he
            if status and status >= 400 and status < 500 and status != 404:
                raise
        except Exception as e:
            print(f"[Deepak] fetch_groww_quote error for {url}:", e)
            last_exc = e
    if last_exc:
        raise last_exc
    else:
        raise Exception("No Groww endpoints to try")

# ========== OPENAI CALL ==========
def ask_openai_for_decision(market_snapshot, symbol):
    if not OPENAI_KEY:
        print("[Deepak] OPENAI_KEY not set. Returning safe FLAT decision.")
        return (
            {"decision": "FLAT", "instrument": symbol, "qty": 0, "entry_price": None, "stoploss": None,
             "rationale": "OpenAI key missing. Defaulting to FLAT.", "confidence_percent": 0},
            "OpenAI key missing"
        )

    system = (
        "You are Deepak Lab assistant. Output STRICT JSON only with keys: "
        '{"decision","instrument","qty","entry_price","stoploss","rationale","confidence_percent"}. '
        "Decision must be exactly one of: BUY, SELL, FLAT. Observe max loss limit and single-active-trade preference. "
        "Trading is manual — do NOT place orders. If unsure, return FLAT."
    )
    user = f"Market snapshot for {symbol} at {datetime.now().isoformat()}:\n{json.dumps(market_snapshot)}\nReturn STRICT JSON."

    headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": OPENAI_TEMPERATURE,
        "max_tokens": 500
    }
    try:
        res = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=20)
        res.raise_for_status()
        data = res.json()
        ai_text = data["choices"][0]["message"]["content"].strip()
        try:
            ai_json = json.loads(ai_text)
        except Exception:
            ai_json = {"decision": "FLAT", "instrument": symbol, "qty": 0, "entry_price": None, "stoploss": None,
                       "rationale": ai_text, "confidence_percent": 0}
        return ai_json, ai_text
    except Exception as e:
        print("[Deepak] OpenAI call failed:", e)
        return {"decision": "FLAT", "instrument": symbol, "qty": 0, "entry_price": None, "stoploss": None,
                "rationale": f"OpenAI error: {e}", "confidence_percent": 0}, str(e)

# ========== JOB HANDLING ==========
def job(symbols=None):
    if symbols is None:
        symbols = SYMBOLS
    for sym in symbols:
        ts = datetime.now().isoformat()
        try:
            snap = fetch_groww_quote(sym)
        except Exception as e:
            print("[Deepak] fetch_groww_quote error:", e)
            snap = {"error": str(e)}
        ai_json, ai_raw = ask_openai_for_decision(snap, sym)

        try:
            cur = DB.cursor()
            cur.execute(
                "INSERT INTO decisions(ts,symbol,market_snapshot,ai_json,ai_raw) VALUES (?,?,?,?,?)",
                (ts, sym, json.dumps(snap), json.dumps(ai_json), ai_raw[:2000])
            )
            DB.commit()
        except Exception as e:
            print("[Deepak] DB write error:", e)

        brief = f"[Deepak Watchdog] {ts} | {sym} | decision={ai_json.get('decision')} | qty={ai_json.get('qty')}"
        print(brief)
        try:
            telegram_notify(brief)
        except Exception as e:
            print("[Notify] telegram failed:", e)
        try:
            webhook_notify({"ts": ts, "symbol": sym, "ai": ai_json})
        except Exception as e:
            print("[Notify] webhook failed:", e)

# ========== FLASK APP & ADMIN CONTROL ==========
app = Flask(__name__)
SCHED = None

@app.route("/")
def health():
    return "ok", 200

@app.route("/latest")
def latest():
    cur = DB.cursor()
    cur.execute("SELECT ts, symbol, market_snapshot, ai_json FROM decisions ORDER BY id DESC LIMIT 1")
    r = cur.fetchone()
    if not r:
        return jsonify({"error": "no data"}), 404
    ts, sym, snap, ai_json = r
    try:
        snap_obj = json.loads(snap)
    except:
        snap_obj = snap
    try:
        ai_obj = json.loads(ai_json)
    except:
        ai_obj = ai_json
    return jsonify({"ts": ts, "symbol": sym, "market_snapshot": snap_obj, "ai": ai_obj}), 200

def _check_admin_token(req):
    token = req.args.get("token", "")
    if not ADMIN_TOKEN:
        return False, ("admin token not set on server", 403)
    if token != ADMIN_TOKEN:
        return False, ("forbidden", 403)
    return True, None

@app.route("/pause")
def pause():
    ok, err = _check_admin_token(request)
    if not ok:
        return err
    global SCHED
    if not SCHED:
        return ("no scheduler", 500)
    SCHED.pause()
    return ("paused", 200)

@app.route("/resume")
def resume():
    ok, err = _check_admin_token(request)
    if not ok:
        return err
    global SCHED
    if not SCHED:
        return ("no scheduler", 500)
    SCHED.resume()
    return ("resumed", 200)

@app.route("/run-now")
def run_now():
    ok, err = _check_admin_token(request)
    if not ok:
        return err
    try:
        job(SYMBOLS)
        return ("job triggered", 200)
    except Exception as e:
        return (f"job error: {e}", 500)

# ========== SCHEDULER START ==========
def start_scheduler():
    global SCHED
    SCHED = BackgroundScheduler(timezone=TIMEZONE)
    SCHED.add_job(job, "interval", seconds=POLL_INTERVAL_SECONDS, args=[SYMBOLS])
    try:
        refresh_hours = float(os.getenv("GROWW_REFRESH_HOURS", str(GROWW_REFRESH_HOURS)))
    except:
        refresh_hours = GROWW_REFRESH_HOURS
    SCHED.add_job(lambda: refresh_groww_token_if_needed(), "interval", hours=refresh_hours)
    SCHED.start()
    print(f"[Deepak] Scheduler started: polling every {POLL_INTERVAL_SECONDS}s; token refresh every {refresh_hours}h")

# ========== APP ENTRYPOINT ==========
if __name__ == "__main__":
    print("[Deepak] Starting watchdog. Symbols:", SYMBOLS, "Polls every:", POLL_INTERVAL_SECONDS)
    try:
        start_scheduler()
    except Exception as e:
        print("[Deepak] Scheduler failed to start:", e)
    app.run(host="0.0.0.0", port=port), port = int(os.environ.get("PORT", 5000))
