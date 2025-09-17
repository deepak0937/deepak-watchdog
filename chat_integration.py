#!/usr/bin/env python3
"""
Fetch latest forecast from Postgres, build a concise prompt, call OpenAI,
and store the reply in raw_logs. Minimal, deterministic (temperature=0).
"""
import os, json, textwrap
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI

DB_DSN = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not DB_DSN or not OPENAI_API_KEY:
    raise RuntimeError("DATABASE_URL and OPENAI_API_KEY must be set in env")

client = OpenAI(api_key=OPENAI_API_KEY)

def connect_db():
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)

def fetch_latest_forecast(limit=1):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, created_at, bias, index_snapshot, oi_summary
        FROM forecasts
        ORDER BY created_at DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def shrink_index_snapshot(idx_json):
    if not idx_json: return {}
    keys = ['last_price','open','high','low','previous_close','volume','timestamp']
    return {k: idx_json.get(k) for k in keys if k in idx_json}

def format_oi_summary(oi_json, top_n=5):
    out = {}
    out['top_ce'] = oi_json.get('top_ce', [])[:top_n]
    out['top_pe'] = oi_json.get('top_pe', [])[:top_n]
    ce_total = sum([v for (_, v) in out['top_ce']]) if out['top_ce'] else 0
    pe_total = sum([v for (_, v) in out['top_pe']]) if out['top_pe'] else 0
    out['ce_total_top'] = ce_total
    out['pe_total_top'] = pe_total
    out['net_top_oi'] = ce_total - pe_total
    return out

def build_messages(forecast_row):
    idx = shrink_index_snapshot(forecast_row['index_snapshot'])
    oi = format_oi_summary(forecast_row['oi_summary'] or {})
    context = {
        "forecast_id": forecast_row["id"],
        "created_at": str(forecast_row["created_at"]),
        "bias": forecast_row.get("bias"),
        "index": idx,
        "oi": oi
    }
    system_prompt = textwrap.dedent("""
    You are Deepak's trading assistant. Use the JSON context to give exactly one clear decision:
    - Provide only: BUY / SELL / NO-TRADE.
    - If BUY/SELL include instrument, entry, stop, risk_estimate_rupees, one_line_rationale.
    - Keep output as strict JSON (see user message).
    - Respect max loss ₹11,000: if position risk could exceed, answer NO-TRADE.
    """).strip()

    user_prompt = "Market context (JSON):\n" + json.dumps(context, default=str, indent=2) + """

Output format (strict JSON):
{
  "decision": "BUY" | "SELL" | "NO-TRADE",
  "instrument": "<symbol or description>",
  "entry": "<price or range>",
  "stop": "<price or range>",
  "risk_estimate_rupees": "<approx rupee risk if taken>",
  "one_line_rationale": "<single sentence>"
}
"""

    return [
        {"role":"system","content":system_prompt},
        {"role":"user","content":user_prompt},
    ]

def call_openai(messages):
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.0,
        max_tokens=400
    )
    # adapt to SDK response shape
    return resp.choices[0].message["content"]

def save_raw_log(payload):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("INSERT INTO raw_logs (payload) VALUES (%s) RETURNING id", (json.dumps(payload, default=str),))
    lid = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return lid

def run_once():
    rows = fetch_latest_forecast(1)
    if not rows:
        print("no forecasts found")
        return
    row = rows[0]
    messages = build_messages(row)
    try:
        reply = call_openai(messages)
    except Exception as e:
        reply = f"ERROR_CALLING_OPENAI: {e}"
    log_id = save_raw_log({"prompt_messages": messages, "reply": reply, "forecast_id": row["id"], "t": str(datetime.utcnow())})
    print("forecast_id:", row["id"], "log_id:", log_id)
    return {"forecast_id": row["id"], "reply": reply, "log_id": log_id}

if __name__ == "__main__":
    print(run_once())
