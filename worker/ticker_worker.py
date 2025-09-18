# worker/ticker_worker.py
from kiteconnect import KiteTicker
import os, redis, json, logging
from services import zerodha

logging.basicConfig(level=logging.INFO)
API_KEY = os.environ["ZERODHA_API_KEY"]
ACCESS_TOKEN = os.environ.get("ZERODHA_ACCESS_TOKEN") or os.environ.get("ZERODHA_ACCESS_TOKEN_REDIS")
kws = KiteTicker(API_KEY, ACCESS_TOKEN)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

RECENT_TICKS_KEY = "RECENT_TICKS"

def on_ticks(ws, ticks):
    # push ticks into redis (as JSON strings)
    r.lpush(RECENT_TICKS_KEY, json.dumps(ticks))
    r.ltrim(RECENT_TICKS_KEY, 0, 199)
    logging.info("pushed %d ticks", len(ticks))

def on_connect(ws, response):
    # Replace tokens with actual instrument_token integers (from instrument master)
    tokens = os.environ.get("SUBSCRIBE_TOKENS", "")  # CSV of instrument_tokens, e.g., "12345,67890"
    token_list = [int(t) for t in tokens.split(",") if t.strip().isdigit()]
    if token_list:
        ws.subscribe(token_list)
        ws.set_mode(ws.MODE_FULL, token_list)
        logging.info("Subscribed to tokens: %s", token_list)
    else:
        logging.warning("No SUBSCRIBE_TOKENS set. Set env var SUBSCRIBE_TOKENS to instrument tokens.")

kws.on_ticks = on_ticks
kws.on_connect = on_connect

if __name__ == "__main__":
    kws.connect(threaded=False)
