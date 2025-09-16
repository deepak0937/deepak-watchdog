"""
deepak_watchdog.py
Safe automation for manual trading:
- Poll Groww for market snapshot
- Ask OpenAI (ChatGPT) for single decision (BUY/SELL/FLAT) in strict JSON
- Log to SQLite
- Notify you via Telegram / webhook / email
- NEVER places orders (trading is manual)
"""

import os
import json
import sqlite3
import requests
from datetime import datetime
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ========== CONFIG ==========
GROWW_KEY = os.getenv("GROWW_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
TIMEZONE = os.getenv("TZ", "Asia/Kolkata")
MAX_LOSS_RUPEES = int(os.getenv("MAX_LOSS_RUPEES", "11000"))
DB_PATH = os.getenv("DB_PATH", "deepak_watchdog.db")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "NIFTY").split(",")]

GROWW_BASE = os.getenv("GROWW_BASE", "https://api.groww.in/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.0"))

# ========== DB ==========
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
    conn.commit()
    return conn

DB = init_db()

# ========== Notify ==========
def telegram_notify(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        print("telegram error:", e)

def webhook_notify(payload):
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=8)
    except Exception as e:
        print("webhook error:", e)

# ========== Groww fetch ==========
def fetch_groww_quote(symbol="NIFTY"):
    url = f"{GROWW_BASE}/live-data/quote"
    headers = {"Authorization": f"Bearer {GROWW_KEY}"}
    params = {"symbol": symbol}
    r = requests.get(url, headers=headers, params=params, timeout=8)
    r.raise_for_status()
    return r.json()

# ========== OpenAI ==========
def ask_openai(snapshot, symbol):
    system = (
        "You are Deepak Lab. Output STRICT JSON only with keys: "
        "{decision, instrument, qty, entry_price, stoploss, rationale, confidence_percent}. "
        f"Decision must be BUY, SELL, or FLAT. Max loss limit is ₹{MAX_LOSS_RUPEES}. "
        "Trading is manual — never place orders. If unsure, return FLAT."
    )

    user = f"Market snapshot for {symbol}:\n{json.dumps(snapshot)}\nReturn STRICT JSON."

    headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type":"application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role":"system","content":system}, {"role":"user","content":user}],
        "temperature": OPENAI_TEMPERATURE,
        "max_tokens": 400
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()
    ai_text = data["choices"][0]["message"]["content"].strip()
    try:
        ai_json = json.loads(ai_text)
    except:
        ai_json = {"decision":"FLAT","instrument":symbol,"qty":0,"entry_price":None,"stoploss":None,"rationale":ai_text,"confidence_percent":0}
    return ai_json, ai_text

# ========== Job ==========
def job(symbols=None):
    if symbols is None:
        symbols = ["NIFTY"]
    for sym in symbols:
        ts = datetime.now().isoformat()
        try:
            snap = fetch_groww_quote(sym)
            ai_json, ai_raw = ask_openai(snap, sym)
        except Exception as e:
            print(f"error {sym}:", e)
            ai_json = {"decision":"FLAT","instrument":sym,"qty":0,"entry_price":None,"stoploss":None,"rationale":str(e),"confidence_percent":0}
            ai_raw = str(e)

        cur = DB.cursor()
        cur.execute("INSERT INTO decisions(ts,symbol,market_snapshot,ai_json,ai_raw) VALUES (?,?,?,?,?)",
                    (ts, sym, json.dumps(snap), json.dumps(ai_json), ai_raw))
        DB.commit()

        brief = f"[Deepak Watchdog] {ts}\n{sym}: {ai_json}"
        print(brief)
        telegram_notify(brief)
        webhook_notify({"ts":ts, "symbol":sym, "ai":ai_json})

# ========== Flask ==========
app = Flask(__name__)

@app.route("/")
def health():
    return "ok", 200

@app.route("/latest")
def latest():
    cur = DB.cursor()
    cur.execute("SELECT ts,symbol,ai_json FROM decisions ORDER BY id DESC LIMIT 1")
    r = cur.fetchone()
    if not r:
        return jsonify({"error":"no data"}), 404
    ts, sym, ai_json = r
    return jsonify({"ts":ts, "symbol":sym, "ai":json.loads(ai_json)})

# ========== Scheduler ==========
def start_scheduler():
    sched = BackgroundScheduler(timezone=TIMEZONE)
    sched.add_job(job, "interval", seconds=POLL_INTERVAL_SECONDS, args=[SYMBOLS])
    sched.start()
    print("Scheduler started every", POLL_INTERVAL_SECONDS, "seconds")

if __name__ == "__main__":
    start_scheduler()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

