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
        # app/services/zerodha.py
import os
import logging
from kiteconnect import KiteConnect
from dotenv import load_dotenv
import redis
import time
from typing import Optional, Dict, Any, List

load_dotenv()
logger = logging.getLogger("deepak_watchdog.zerodha")

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

# Redis key names (used by main.py as well)
REDIS_ACCESS_KEY = "ZERODHA_ACCESS_TOKEN"  # note: main.py uses this exact name

def _kite_client(access_token: Optional[str] = None) -> KiteConnect:
    """Return a KiteConnect instance. If access_token provided, set it."""
    if not KITE_API_KEY:
        raise RuntimeError("KITE_API_KEY not configured")
    kc = KiteConnect(api_key=KITE_API_KEY)
    if access_token:
        kc.set_access_token(access_token)
    else:
        # if no access token provided, try redis
        token = r.get(REDIS_ACCESS_KEY)
        if token:
            kc.set_access_token(token)
    return kc

# -------- auth helpers --------
def get_login_url() -> str:
    """
    Returns the Zerodha login URL that the user must open to login and obtain request_token.
    Make sure your app's redirect URL in Kite developer console matches /callback/zerodha.
    """
    if not KITE_API_KEY:
        raise RuntimeError("KITE_API_KEY missing")
    kc = KiteConnect(api_key=KITE_API_KEY)
    return kc.login_url()

def generate_session(request_token: str) -> dict:
    """
    Exchange request_token (from redirect/callback) for access_token.
    Returns the kite session dict including access_token.
    """
    if not request_token:
        raise ValueError("request_token required")
    kc = KiteConnect(api_key=KITE_API_KEY)
    data = kc.generate_session(request_token, api_secret=KITE_API_SECRET)
    # Save to redis for later use
    access_token = data.get("access_token")
    if access_token:
        r.set(REDIS_ACCESS_KEY, access_token)
        # optional: save timestamp
        r.set(f"{REDIS_ACCESS_KEY}:ts", int(time.time()))
    return data

def set_access_token(access_token: str) -> None:
    """Explicitly set the access_token in redis (used by main.py)."""
    if not access_token:
        raise ValueError("access_token required")
    r.set(REDIS_ACCESS_KEY, access_token)
    r.set(f"{REDIS_ACCESS_KEY}:ts", int(time.time()))

# -------- market data helpers --------
def get_ltp(exchange: str, symbol: str) -> Dict[str, Any]:
    """
    Return LTP for a given exchange and symbol.
    Example: get_ltp("NSE", "RELIANCE") or get_ltp("NSE", "NIFTY 50")
    Note: some indices have slightly different naming on Kite; fallback to simple attempts.
    """
    kc = _kite_client()
    # Build common symbol variants to try
    tries = []
    # If user passed index like "NIFTY 50" allow "NSE:NIFTY 50" and "NSE:NIFTY"
    symbol_clean = symbol.strip()
    tries.append(f"{exchange}:{symbol_clean}")
    if " " in symbol_clean:
        tries.append(f"{exchange}:{symbol_clean.replace(' ', '')}")
    # also try uppercase without spaces
    tries.append(f"{exchange}:{symbol_clean.upper().replace(' ', '')}")
    last_err = None
    for t in tries:
        try:
            data = kc.ltp(t)
            # kc.ltp returns dict keyed by the instrument string
            return data.get(t, data)
        except Exception as e:
            last_err = e
            logger.debug("ltp try failed for %s: %s", t, e)
    raise RuntimeError(f"LTP fetch failed for {symbol} (tries: {tries}). last_err: {last_err}")

def get_option_chain(symbol: str, strikes_range: int = 500, step: int = 50) -> Dict[str, Any]:
    """
    Build a local option chain snapshot around ATM.
    - symbol: "NIFTY50" or "NIFTY" (main function callers pass "NIFTY50" etc).
    - strikes_range: how far above/below ATM to query (default 500)
    - step: strike step (50 for NIFTY)
    Returns dict of CE/PE LTP keyed by strike.
    """
    kc = _kite_client()
    # Get spot first
    try:
        spot_data = get_ltp("NSE", symbol)
        spot = float(spot_data.get("last_price", 0) or spot_data.get("last_price", 0.0))
    except Exception as e:
        logger.warning("unable to fetch spot for option chain: %s", e)
        raise

    # Round ATM to nearest 'step'
    atm = int(round(spot / step) * step)
    strikes = list(range(atm - strikes_range, atm + strikes_range + step, step))
    result = {"spot": spot, "atm": atm, "strikes": {}}

    for st in strikes:
        ce_sym = f"NFO:{symbol}{st}CE"
        pe_sym = f"NFO:{symbol}{st}PE"
        # try fetch ltp; if fails, continue
        entry = {}
        try:
            ce = kc.ltp(ce_sym)
            entry["CE"] = ce.get(ce_sym, ce)
        except Exception:
            entry["CE"] = None
        try:
            pe = kc.ltp(pe_sym)
            entry["PE"] = pe.get(pe_sym, pe)
        except Exception:
            entry["PE"] = None
        result["strikes"][st] = entry
    return result

def get_positions() -> List[Dict[str, Any]]:
    """Return current positions (portfolio) via kite.positions()."""
    kc = _kite_client()
    return kc.positions()

# -------- order placement helper --------
def place_market_order(exchange: str, tradingsymbol: str, qty: int, transaction_type: str,
                       product: str = "MIS", simulate: bool = False, order_type: str = "MARKET",
                       price: Optional[float] = None) -> Dict[str, Any]:
    """
    Place a market (or limit if order_type provided) order. If simulate=True, return a mock.
    Parameters align with main.place_trade_internal usage.
    """
    if simulate:
        # return a deterministic simulated response
        return {
            "order_id": f"SIM-{int(time.time())}",
            "status": "simulated",
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": qty,
            "product": product,
            "order_type": order_type,
        }

    kc = _kite_client()
    params = {
        "exchange": exchange,
        "tradingsymbol": tradingsymbol,
        "transaction_type": transaction_type,
        "quantity": qty,
        "product": product,
        "order_type": order_type,
    }
    if order_type.upper() == "LIMIT" and price is not None:
        params["price"] = price

    # place_order returns an order_id / acknowledgement dict
    resp = kc.place_order(**params)
    return resp

