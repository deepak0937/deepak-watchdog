# main.py
import os
import json
import time
import re
import logging
from fastapi import FastAPI, Request, Header, HTTPException, Query
import redis
from services import zerodha
from predictor.predictor import get_prediction

# -------- logging --------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deepak_watchdog")

# -------- app & redis --------
app = FastAPI(title="Deepak Watchdog API")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")
MAX_ALLOWED_LOSS = int(os.environ.get("MAX_ALLOWED_LOSS", "11000"))  # rupees
ACTIVE_TRADE_KEY = "ACTIVE_TRADE"
PREDICTIONS_LIST = "PREDICTIONS"


# -------- helpers --------
def check_admin(token: str):
    if token != ADMIN_TOKEN:
        logger.warning("unauthorized admin token attempt")
        raise HTTPException(status_code=401, detail="unauthorized")


# -------- health --------
@app.get("/health")
def health():
    return {"status": "ok", "time": time.time()}


# -------- zerodha auth endpoints --------
@app.get("/login/zerodha")
def login_zerodha():
    try:
        url = zerodha.get_login_url()
        return {"login_url": url}
    except Exception as e:
        logger.exception("failed to generate login url")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/callback/zerodha")
def cb_zerodha(request: Request):
    rt = request.query_params.get("request_token")
    if not rt:
        raise HTTPException(status_code=400, detail="no request_token")
    try:
        data = zerodha.generate_session(rt)
        access_token = data.get("access_token")
        if access_token:
            zerodha.set_access_token(access_token)
            # persist access token in redis (or DB as you prefer)
            r.set("ZERODHA_ACCESS_TOKEN", access_token)
            logger.info("Zerodha access token saved to redis")
        return {"status": "ok", "data": data}
    except Exception as e:
        logger.exception("failed to generate session from request_token")
        raise HTTPException(status_code=500, detail=str(e))


# -------- prediction endpoint --------
@app.post("/predict")
def predict(x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    raw_pred = None
    try:
        raw_pred = get_prediction()

        pred = None
        if isinstance(raw_pred, str):
            cleaned = raw_pred

            # 1) Remove any fenced-blocks (```json ... ```)
            def _unfence(m):
                inner = m.group(0)
                inner = re.sub(r"^```(?:\s*\w+\s*)?", "", inner, flags=re.I)
                inner = inner.rsplit("```", 1)[0]
                return inner

            cleaned = re.sub(r"```[\s\S]*?```", _unfence, cleaned, flags=re.I).strip()
            cleaned = cleaned.replace("```", "").replace("`", "").replace("json", "").strip()

            # 2) Try JSON parse directly
            try:
                pred = json.loads(cleaned)
            except Exception:
                # 3) Fallback: extract first {...}
                try:
                    start = cleaned.index("{")
                    end = cleaned.rindex("}") + 1
                    candidate = cleaned[start:end]
                    pred = json.loads(candidate)
                except Exception:
                    pred = None
        else:
            pred = raw_pred

        if pred is None:
            pred = {"error": "unparseable", "raw": raw_pred if raw_pred is not None else ""}

        log = {"ts": time.time(), "prediction": pred}
        try:
            r.lpush(PREDICTIONS_LIST, json.dumps(log))
            logger.info("prediction stored")
        except Exception:
            logger.exception("failed to push prediction to redis")

        return {"status": "ok", "source": "openai", "data": pred}

    except Exception as exc:
        logger.exception("predict endpoint error")
        fallback = {"error": "prediction_exception", "detail": str(exc)}
        if raw_pred is not None:
            fallback["raw"] = raw_pred
        return {"status": "ok", "source": "openai", "data": fallback}


# -------- trade simulation & placement --------
@app.post("/simulate_trade")
def simulate_trade(payload: dict, x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    return place_trade_internal(payload, simulate=True)


@app.post("/trade")
def trade(payload: dict, x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    if r.exists(ACTIVE_TRADE_KEY):
        logger.info("attempt to place trade blocked by active trade rule")
        return {"status": "blocked", "reason": "single active trade exists"}
    resp = place_trade_internal(payload, simulate=False)
    if resp.get("status") == "placed":
        r.set(
            ACTIVE_TRADE_KEY,
            json.dumps({"placed_resp": resp["resp"], "ts": time.time()})
        )
        logger.info("active trade recorded in redis")
    return resp


def place_trade_internal(payload: dict, simulate: bool = False) -> dict:
    required = ("exchange", "tradingsymbol", "qty", "transaction_type", "entry", "stoploss")
    for k in required:
        if k not in payload:
            logger.warning("missing field in payload: %s", k)
            return {"status": "rejected", "reason": f"missing_{k}"}

    try:
        entry = float(payload["entry"])
        stop = float(payload["stoploss"])
        qty = int(payload["qty"])
        lot_size = int(payload.get("lot_size", 1))
    except Exception as e:
        logger.warning("invalid payload types")
        return {"status": "rejected", "reason": "invalid_payload_types", "detail": str(e)}

    worst_loss = abs(entry - stop) * qty * lot_size
    if worst_loss > MAX_ALLOWED_LOSS:
        logger.info("rejected trade: worst_loss=%s exceeds max=%s", worst_loss, MAX_ALLOWED_LOSS)
        return {"status": "rejected", "reason": "worst_case_loss_exceeds_limit", "worst_loss": worst_loss}

    try:
        order_resp = zerodha.place_market_order(
            exchange=payload["exchange"],
            tradingsymbol=payload["tradingsymbol"],
            qty=qty,
            transaction_type=payload["transaction_type"],
            product=payload.get("product", "MIS"),
            simulate=simulate
        )
    except Exception as e:
        logger.exception("order placement failed")
        return {"status": "failed", "reason": str(e)}

    if simulate:
        return {"status": "simulated", "resp": order_resp, "worst_loss": worst_loss}
    return {"status": "placed", "resp": order_resp, "worst_loss": worst_loss}


# -------- debug / admin helpers --------
@app.get("/admin/active_trade")
def get_active_trade(x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    val = r.get(ACTIVE_TRADE_KEY)
    return {"active_trade": json.loads(val) if val else None}


@app.post("/admin/clear_active_trade")
def clear_active_trade(x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    r.delete(ACTIVE_TRADE_KEY)
    logger.info("active trade cleared by admin")
    return {"status": "cleared"}


# -------- zerodha snapshot endpoint --------
@app.get("/zerodha/snapshot")
def zerodha_snapshot(
    x_admin_token: str = Header(None),
    symbol: str = Query("NIFTY50", description="Tradingsymbol (e.g., NIFTY50, BANKNIFTY)")
):
    """
    Returns live Zerodha data (LTP + Option Chain if available).
    Example:
    curl -X GET "https://deepak-watchdog.onrender.com/zerodha/snapshot?symbol=NIFTY50" -H "x-admin-token: TOKEN"
    """
    check_admin(x_admin_token)
    try:
        # 🔹 LTP
        ltp = zerodha.get_ltp("NSE", symbol)

        # 🔹 Option Chain
        option_chain = {}
        try:
            option_chain = zerodha.get_option_chain(symbol)
        except Exception as oc_err:
            logger.warning("option chain not available: %s", oc_err)

        # 🔹 Positions
        positions = {}
        try:
            positions = zerodha.get_positions()
        except Exception as pos_err:
            logger.warning("positions fetch failed: %s", pos_err)

        return {
            "status": "ok",
            "symbol": symbol,
            "ltp": ltp,
            "option_chain": option_chain,
            "positions": positions,
            "ts": time.time()
        }

    except Exception as e:
        logger.exception("snapshot error")
        return {"status": "error", "detail": str(e)}
        # -------- deepak trend+ endpoint --------
@app.get("/deepak-trend")
def deepak_trend(x_admin_token: str = Header(None)):
    """
    Deepak Trend+ analysis input endpoint.
    Pulls NIFTY spot, India VIX, option OI (ATM CE/PE), and returns snapshot.
    """
    check_admin(x_admin_token)

    try:
        # Spot & Futures
        spot = zerodha.get_ltp("NSE", "NIFTY 50")

        # India VIX
        vix = zerodha.get_ltp("NSE", "INDIAVIX")

        # 🔹 Auto-detect ATM strike (round to nearest 50)
        spot_val = float(spot.get("last_price", 0))
        atm_strike = int(round(spot_val / 50) * 50)

        ce_symbol = f"NIFTY{atm_strike}CE"
        pe_symbol = f"NIFTY{atm_strike}PE"

        option_chain = {}
        try:
            ce_data = zerodha.get_ltp("NFO", ce_symbol)
            pe_data = zerodha.get_ltp("NFO", pe_symbol)
            option_chain = {ce_symbol: ce_data, pe_symbol: pe_data}
        except Exception as oc_err:
            logger.warning("ATM option fetch failed: %s", oc_err)

        return {
            "status": "ok",
            "spot": spot,
            "vix": vix,
            "atm_strike": atm_strike,
            "option_chain": option_chain,
            "ts": time.time()
        }

    except Exception as e:
        logger.exception("deepak trend+ error")
        return {"status": "error", "detail": str(e)}
