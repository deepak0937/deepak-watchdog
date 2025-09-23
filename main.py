# main.py
import os
import json
import time
import re
import logging
from fastapi import FastAPI, Request, Header, HTTPException, Query

import redis  # redis-py (synchronous client)

from services import zerodha
from predictor.predictor import get_prediction

# -------- logging --------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deepak_watchdog")

# -------- app --------
app = FastAPI(title="Deepak Watchdog API")
_start_ts = time.time()

# config / env
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")
MAX_ALLOWED_LOSS = int(os.environ.get("MAX_ALLOWED_LOSS", "11000"))  # rupees
ACTIVE_TRADE_KEY = "ACTIVE_TRADE"
PREDICTIONS_LIST = "PREDICTIONS"


# -------- lifecycle: startup / shutdown --------
@app.on_event("startup")
def startup_event():
    """
    Create per-worker Redis client here (NOT at module import time).
    This ensures each worker has its own connections and avoids FD sharing
    issues that lead to Errno 9 on shutdown under Gunicorn.
    """
    try:
        app.state.r = redis.from_url(REDIS_URL, decode_responses=True)
        logger.info("redis client created for worker")
    except Exception as e:
        # If Redis isn't required for some endpoints, we still allow the app to start.
        app.state.r = None
        logger.exception("failed to create redis client at startup")


@app.on_event("shutdown")
def shutdown_event():
    """
    Close/cleanup redis client cleanly.
    """
    try:
        rclient = getattr(app.state, "r", None)
        if rclient:
            try:
                # Close high-level client and disconnect underlying connections
                rclient.close()
            except Exception:
                # older/newer redis clients may use connection_pool.disconnect()
                try:
                    pool = getattr(rclient, "connection_pool", None)
                    if pool:
                        pool.disconnect()
                except Exception as ex:
                    logger.exception("failed disconnecting redis pool: %s", ex)
            logger.info("redis client closed on shutdown")
    except Exception as exc:
        logger.exception("error during shutdown cleanup: %s", exc)


# -------- helpers --------
def check_admin(token: str):
    if token != ADMIN_TOKEN:
        logger.warning("unauthorized admin token attempt")
        raise HTTPException(status_code=401, detail="unauthorized")


def _r():
    """
    Helper to fetch redis client from app state and raise an error
    if not available. Use this to keep code compact.
    """
    rc = getattr(app.state, "r", None)
    if rc is None:
        raise HTTPException(status_code=503, detail="redis not available")
    return rc


# -------- health --------
@app.get("/health")
def health():
    uptime = int(time.time() - _start_ts)
    return {"status": "ok", "uptime_seconds": uptime, "ts": time.time()}


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
            # persist access token in redis (if available)
            try:
                rclient = _r()
                rclient.set("ZERODHA_ACCESS_TOKEN", access_token)
                logger.info("Zerodha access token saved to redis")
            except HTTPException:
                logger.warning("redis not available; skipping token persist")
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
            try:
                rclient = _r()
                rclient.lpush(PREDICTIONS_LIST, json.dumps(log))
                logger.info("prediction stored")
            except HTTPException:
                logger.warning("redis not available; skipping prediction store")
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
    try:
        rclient = _r()
        if rclient.exists(ACTIVE_TRADE_KEY):
            logger.info("attempt to place trade blocked by active trade rule")
            return {"status": "blocked", "reason": "single active trade exists"}
    except HTTPException:
        # if redis not available, continue but log (depends on your policy)
        logger.warning("redis not available when checking active trade; allowing placement")

    resp = place_trade_internal(payload, simulate=False)
    if resp.get("status") == "placed":
        try:
            rclient = _r()
            rclient.set(
                ACTIVE_TRADE_KEY,
                json.dumps({"placed_resp": resp["resp"], "ts": time.time()})
            )
            logger.info("active trade recorded in redis")
        except HTTPException:
            logger.warning("redis not available; active trade not recorded")
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
    try:
        rclient = _r()
        val = rclient.get(ACTIVE_TRADE_KEY)
        return {"active_trade": json.loads(val) if val else None}
    except HTTPException:
        logger.warning("redis not available; cannot fetch active trade")
        return {"active_trade": None}


@app.post("/admin/clear_active_trade")
def clear_active_trade(x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    try:
        rclient = _r()
        rclient.delete(ACTIVE_TRADE_KEY)
        logger.info("active trade cleared by admin")
    except HTTPException:
        logger.warning("redis not available; clear_active_trade noop")
    return {"status": "cleared"}


# -------- manual admin: set zerodha token --------
@app.post("/admin/set_zerodha_token")
async def admin_set_token(request: Request, x_admin_token: str = Header(None)):
    check_admin(x_admin_token)
    body = await request.json()
    token = body.get("access_token")

    if not token:
        raise HTTPException(status_code=400, detail="missing access_token")

    try:
        zerodha.set_access_token(token)   # writes to Redis (if your zerodha service uses redis)
        logger.info("Zerodha access token manually set via admin")
        try:
            rclient = _r()
            rclient.set("ZERODHA_ACCESS_TOKEN", token)
        except HTTPException:
            logger.warning("redis not available; skipping persisting token")
        return {"status": "ok", "message": "token updated", "token": token}
    except Exception as e:
        logger.exception("failed to set zerodha token")
        raise HTTPException(status_code=500, detail=str(e))


# -------- zerodha snapshot endpoint --------
@app.get("/zerodha/snapshot")
def zerodha_snapshot(
    x_admin_token: str = Header(None),
    symbol: str = Query("NIFTY50", description="Tradingsymbol (e.g., NIFTY50, BANKNIFTY)")
):
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


# -------- local dev runner (guarded) --------
if __name__ == "__main__":
    # This block is for local development only.
    # When using Gunicorn with uvicorn workers, Gunicorn will import this module
    # and run the app; it will NOT execute this block.
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
