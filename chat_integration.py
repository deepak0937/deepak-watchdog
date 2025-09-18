#!/usr/bin/env python3
"""
Fetch latest forecast from Postgres, build a concise prompt, call OpenAI,
and store the reply in raw_logs. Deterministic (temperature=0).
Improved: uses context managers, safer OpenAI response handling, basic logging.
"""
import os
import json
import textwrap
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI  # new-style OpenAI client; if you use 'openai' package, adapt accordingly

# ---- config ----
DB_DSN = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "400"))

if not DB_DSN or not OPENAI_API_KEY:
    raise RuntimeError("DATABASE_URL and OPENAI_API_KEY must be set in env")

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("forecast_logger")

# ---- OpenAI client ----
client = OpenAI(api_key=OPENAI_API_KEY)

# ---- DB helpers ----
def connect_db():
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)

def fetch_latest_forecast(limit: int = 1) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, created_at, bias, index_snapshot, oi_summary
        FROM forecasts
        ORDER BY created_at DESC
        LIMIT %s
    """
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
    return rows

def save_raw_log(payload: Dict[str, Any]) -> int:
    sql = "INSERT INTO raw_logs (payload) VALUES (%s) RETURNING id"
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (json.dumps(payload, default=str),))
            lid = cur.fetchone()[0]
        conn.commit()
    return lid

# ---- data formatting helpers ----
def shrink_index_snapshot(idx_json: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not idx_json:
        return {}
    keys = ['last_price', 'open', 'high', 'low', 'previous_close', 'volume', 'timestamp']
    return {k: idx_json.get(k) for k in keys if k in idx_json}

def format_oi_summary(oi_json: Any, top_n: int = 5) -> Dict[str, Any]:
    # If oi_json is a string (text column), try to parse it
    if isinstance(oi_json, str):
        try:
            oi_json = json.loads(oi_json)
        except Exception:
            oi_json = {}
    if not oi_json:
        oi_json = {}

    out: Dict[str, Any] = {}
    out['top_ce'] = oi_json.get('top_ce', [])[:top_n]
    out['top_pe'] = oi_json.get('top_pe', [])[:top_n]
    ce_total = sum([v for (_, v) in out['top_ce']]) if out['top_ce'] else 0
    pe_total = sum([v for (_, v) in out['top_pe']]) if out['top_pe'] else 0
    out['ce_total_top'] = ce_total
    out['pe_total_top'] = pe_total
    out['net_top_oi'] = ce_total - pe_total
    return out

def build_messages(forecast_row: Dict[str, Any]) -> List[Dict[str, str]]:
    idx = shrink_index_snapshot(forecast_row.get('index_snapshot') or {})
    oi = format_oi_summary(forecast_row.get('oi_summary') or {})
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
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

# ---- OpenAI call ----
def call_openai(messages: List[Dict[str, str]]) -> str:
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.0,
            max_tokens=OPENAI_MAX_TOKENS
        )
    except Exception as exc:
        logger.exception("OpenAI API call failed")
        raise

    # Defensive handling of different SDK shapes
    try:
        # Newer SDK: resp.choices[0].message["content"]
        content = resp.choices[0].message.get("content") if getattr(resp.choices[0], "message", None) else None
        if not content:
            # Fallback: some SDKs return text in choices[0].text or choices[0].message.content
            content = getattr(resp.choices[0], "text", None) or (resp.choices[0].message.content if getattr(resp.choices[0].message, "content", None) else None)
    except Exception:
        # last-resort: stringify response
        content = str(resp)

    if not content:
        logger.warning("OpenAI returned no content; saving raw resp")
        content = json.dumps(resp, default=str)

    return content

# ---- main run ----
def run_once() -> Dict[str, Any]:
    rows = fetch_latest_forecast(1)
    if not rows:
        logger.info("no forecasts found")
        return {"error": "no_forecasts"}

    row = rows[0]
    messages = build_messages(row)
    reply = None
    try:
        reply = call_openai(messages)
    except Exception as e:
        reply = f"ERROR_CALLING_OPENAI: {e}"
        logger.exception("failed calling OpenAI")

    payload = {
        "prompt_messages": messages,
        "reply": reply,
        "forecast_id": row["id"],
        "t": datetime.utcnow().isoformat()
    }

    try:
        log_id = save_raw_log(payload)
        logger.info("saved raw log id=%s for forecast=%s", log_id, row["id"])
    except Exception:
        # Save failed — log locally and include in return payload
        logger.exception("failed saving raw log to DB")
        log_id = None

    return {"forecast_id": row["id"], "reply": reply, "log_id": log_id}

if __name__ == "__main__":
    print(run_once())

