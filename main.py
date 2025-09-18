# main.py
import os, json, time
from fastapi import FastAPI, Request, Header, HTTPException
import redis
from services import zerodha
from predictor.predictor import get_prediction

app = FastAPI()

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")
MAX_ALLOWED_LOSS = int(os.environ.get("MAX_ALLOWED_LOSS", "11000"))  # rupees
ACTIVE_TRADE_KEY = "ACTIVE_TRADE"
PREDICTIONS_LIST = "PREDICTIONS"

def check_admin(token: str):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

@app.get("/login/zerodha")
def login_zerodha():
    return {"login_url": zerodha.get_login_url()}

@app.get("/callback/zerodha")
def cb_zerodha(request: Request):
    rt = request.query_params.get("request_token")
    if not rt:
        raise HTTPException(status_code=400, detail="no request_token")
    data = zerodha.generate_session(rt)
    access_token = data.get("access_token")
    if access_token:
        zerodha.set_access_token(access_token)
        r.set("ZERODHA_ACCESS_TOKEN", access_token)
    return {"status": "ok", "data": data}

@app.post("/predict")
def predict(x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    pred = get_prediction()
    log = {"ts": time.time(), "prediction": pred}
    r.lpush(PREDICTIONS_LIST, json.dumps(log))
    return pred

@app.post("/simulate_trade")
def simulate_trade(payload: dict, x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    return place_trade_internal(payload, simulate=True)

@app.post("/trade")
def trade(payload: dict, x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    if r.exists(ACTIVE_TRADE_KEY):
        return {"status": "blocked", "reason": "single active trade exists"}
    resp = place_trade_internal(payload, simulate=False)
    if resp.get("status") == "placed":
        r.set(ACTIVE_TRADE_KEY, json.dumps({"placed_resp": resp["resp"], "ts": time.time()}))
    return resp

def place_trade_internal(payload: dict, simulate: bool = False) -> dict:
    """
    Expected payload fields:
      - exchange (e.g., "NSE" or "NFO")
      - tradingsymbol (string)
      - qty (int)           # number of lots or qty depending on instrument
      - transaction_type ("BUY"/"SELL")
      - entry (float)
      - stoploss (float)
      - lot_size (int)      # important for index/options; default 1
      - product (str)       # optional, e.g., "MIS"
    """
    required = ("exchange", "tradingsymbol", "qty", "transaction_type", "entry", "stoploss")
    for k in required:
        if k not in payload:
            return {"status": "rejected", "reason": f"missing_{k}"}

    entry = float(payload["entry"])
    stop = float(payload["stoploss"])
    qty = int(payload["qty"])
    lot_size = int(payload.get("lot_size", 1))

    worst_loss = abs(entry - stop) * qty * lot_size
    if worst_loss > MAX_ALLOWED_LOSS:
        return {"status": "rejected", "reason": "worst_case_loss_exceeds_limit", "worst_loss": worst_loss}

    # place order or simulate
    order_resp = zerodha.place_market_order(
        exchange=payload["exchange"],
        tradingsymbol=payload["tradingsymbol"],
        qty=qty,
        transaction_type=payload["transaction_type"],
        product=payload.get("product", "MIS"),
        simulate=simulate
    )

    if simulate:
        return {"status": "simulated", "resp": order_resp, "worst_loss": worst_loss}
    return {"status": "placed", "resp": order_resp, "worst_loss": worst_loss}
