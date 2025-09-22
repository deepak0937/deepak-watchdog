# app/services/zerodha.py
import os
import logging
import time
from typing import Optional, Dict, Any, List

from kiteconnect import KiteConnect

# optional redis support
try:
    import redis
except Exception:
    redis = None

logger = logging.getLogger("zerodha_service")

# env keys (note: your repo used ZERODHA_* names; keep compatibility)
API_KEY = os.getenv("ZERODHA_API_KEY") or os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("ZERODHA_API_SECRET") or os.getenv("KITE_API_SECRET")

# Redis config
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_ACCESS_KEY = "ZERODHA_ACCESS_TOKEN"  # key used by main.py as well

# Try to initialize redis client if library present
_r = None
if redis is not None:
    try:
        _r = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        _r = None

_kite_client_instance: Optional[KiteConnect] = None


def _get_token_from_redis() -> Optional[str]:
    if not _r:
        return None
    try:
        return _r.get(REDIS_ACCESS_KEY)
    except Exception as e:
        logger.debug("Redis GET failed: %s", e)
        return None


def _save_token_to_redis(token: str) -> None:
    if not _r:
        logger.debug("Redis client not configured; skipping save to redis")
        return
    try:
        _r.set(REDIS_ACCESS_KEY, token)
        _r.set(f"{REDIS_ACCESS_KEY}:ts", int(time.time()))
    except Exception as e:
        logger.exception("Failed to save token to redis: %s", e)


def _kite_client(access_token: Optional[str] = None) -> KiteConnect:
    """
    Return a KiteConnect client. Priority for access_token:
      1) explicit `access_token` passed to this function
      2) token stored in Redis under REDIS_ACCESS_KEY
      3) token present in environment var ZERODHA_ACCESS_TOKEN
    """
    global _kite_client_instance

    if not API_KEY:
        raise RuntimeError("KITE API key not configured (ZERODHA_API_KEY / KITE_API_KEY missing)")

    if _kite_client_instance is None:
        _kite_client_instance = KiteConnect(api_key=API_KEY)

    # 1) explicit param
    if access_token:
        _kite_client_instance.set_access_token(access_token)
        return _kite_client_instance

    # 2) redis
    token = _get_token_from_redis()
    if token:
        _kite_client_instance.set_access_token(token)
        return _kite_client_instance

    # 3) env var fallback
    env_token = os.getenv("ZERODHA_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN")
    if env_token:
        _kite_client_instance.set_access_token(env_token)
        return _kite_client_instance

    # no token available; return client (callers will get clear error from kite lib)
    return _kite_client_instance


# -------- auth / session helpers --------
def get_login_url() -> str:
    kc = _kite_client()
    url = kc.login_url()
    logger.info("Generated Zerodha login URL")
    return url


def generate_session(request_token: str) -> dict:
    """
    Exchange request_token for access_token using API_SECRET.
    Returns the kite session dict.
    """
    if not API_SECRET:
        raise RuntimeError("KITE API secret not configured (ZERODHA_API_SECRET / KITE_API_SECRET missing)")

    kc = _kite_client()
    data = kc.generate_session(request_token, api_secret=API_SECRET)
    token = data.get("access_token")
    if token:
        # set locally and in redis
        set_access_token(token)
        logger.info("Generated and stored new access_token (in process & redis/env fallback).")
    return data


def set_access_token(token: str) -> None:
    """
    Update the in-process kite client with the token and persist to redis if available.
    """
    global _kite_client_instance
    if _kite_client_instance is None:
        _kite_client_instance = KiteConnect(api_key=API_KEY)
    _kite_client_instance.set_access_token(token)
    # save to redis (best-effort)
    try:
        _save_token_to_redis(token)
    except Exception:
        logger.exception("Failed to save token to redis")
    logger.info("Zerodha access_token set in process (and attempt saved to redis).")


# -------- market data helpers --------
def get_ltp(exchange: str, tradingsymbol: str) -> Dict[str, Any]:
    """
    Get last traded price for a symbol. Tries some common key variants.
    """
    kc = _kite_client()
    # Build a few variants that Kite expects
    variants = []
    key_exact = f"{exchange}:{tradingsymbol}"
    variants.append(key_exact)
    if " " in tradingsymbol:
        variants.append(f"{exchange}:{tradingsymbol.replace(' ', '')}")
    variants.append(f"{exchange}:{tradingsymbol.upper().replace(' ', '')}")

    last_err = None
    for key in variants:
        try:
            res = kc.ltp(key)
            return res.get(key, res)
        except Exception as e:
            last_err = e
            logger.debug("ltp try failed for %s: %s", key, e)
    # if all failed, raise a clear error
    raise RuntimeError(f"LTP fetch failed for {tradingsymbol}. Last error: {last_err}")


def get_option_chain(symbol: str, strikes_range: int = 500, step: int = 50) -> Dict[str, Any]:
    """
    Build a small option chain snapshot around ATM. This is a best-effort helper and may be rate-limited.
    """
    kc = _kite_client()
    spot_data = get_ltp("NSE", symbol)
    spot = float(spot_data.get("last_price", 0) or 0.0)
    atm = int(round(spot / step) * step)
    strikes = list(range(max(0, atm - strikes_range), atm + strikes_range + step, step))
    result = {"spot": spot, "atm": atm, "strikes": {}}
    for st in strikes:
        ce_sym = f"NFO:{symbol}{st}CE"
        pe_sym = f"NFO:{symbol}{st}PE"
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
    kc = _kite_client()
    try:
        return kc.positions()
    except Exception as e:
        logger.exception("Failed to fetch positions: %s", e)
        return {}


# -------- order placement helper --------
def place_market_order(exchange: str, tradingsymbol: str, qty: int, transaction_type: str,
                       product: str = "MIS", simulate: bool = False, order_type: str = "MARKET",
                       price: Optional[float] = None) -> Dict[str, Any]:
    """
    Place or simulate a market/limit order.
    """
    if simulate:
        return {
            "order_id": f"SIM-{int(time.time())}",
            "status": "simulated",
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": qty,
            "product": product,
            "order_type": order_type
        }

    kc = _kite_client()
    params = {
        "variety": kc.VARIETY_REGULAR,
        "exchange": exchange,
        "tradingsymbol": tradingsymbol,
        "transaction_type": transaction_type,
        "quantity": qty,
        "product": product,
        "order_type": kc.ORDER_TYPE_MARKET if order_type.upper() == "MARKET" else kc.ORDER_TYPE_LIMIT,
    }
    if order_type.upper() == "LIMIT" and price is not None:
        params["price"] = price

    resp = kc.place_order(**params)
    return resp

