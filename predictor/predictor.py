# predictor/predictor.py
import os, json, time
import openai, redis
openai.api_key = os.environ["OPENAI_API_KEY"]
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

PROMPT_TEMPLATE = """
You are 'Deepak Trend Assistant'. Given the DATA below, return strict JSON ONLY.

DATA:
{data_blob}

REQUIREMENTS:
Return JSON with keys:
- date (YYYY-MM-DD)
- bias: "BULLISH"|"BEARISH"|"NEUTRAL"
- probability_pct: integer 0-100
- pivot: float
- support: [float, float]
- resistance: [float, float]
- reason: short string (1-2 lines)
- trade_suggestion: either null or an object with keys:
    {{ "type": "BUY"/"SELL", "entry":float, "qty":int, "stoploss":float, "target":float, "lot_size":int }}
Return ONLY valid JSON.
"""

def build_data_blob():
    ticks = r.lrange("RECENT_TICKS", 0, 50)
    ticks = [json.loads(t) for t in ticks] if ticks else []
    blob = {
        "recent_ticks_count": len(ticks),
        "sample_tick": ticks[0] if ticks else {},
        "note": "Extend this blob in future with OI, macros (DXY/crude/us futures), IndiaVIX, FII/DII"
    }
    return json.dumps(blob)

def get_prediction():
    data_blob = build_data_blob()
    prompt = PROMPT_TEMPLATE.format(data_blob=data_blob)
    resp = openai.ChatCompletion.create(
        model=MODEL,
        messages=[{"role":"user","content":prompt}],
        temperature=0.0,
        max_tokens=400
    )
    text = resp["choices"][0]["message"]["content"].strip()
    try:
        return json.loads(text)
    except Exception:
        return {"error": "invalid_json", "raw": text}
