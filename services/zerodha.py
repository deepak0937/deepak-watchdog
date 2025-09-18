# services/zerodha.py
import os, json
from kiteconnect import KiteConnect

API_KEY = os.environ["ZERODHA_API_KEY"]
API_SECRET = os.environ["ZERODHA_API_SECRET"]
ACCESS_TOKEN = os.environ.get("ZERODHA_ACCESS_TOKEN")  # optional until callback

kite = KiteConnect(api_key=API_KEY)
if ACCESS_TOKEN:
    kite.set_access_token(ACCESS_TOKEN)

def get_login_url():
    return kite.login_url()

def generate_session(request_token: str) -> dict:
    """
    Exchange request_token for access_token.
    Returns the Kite response dict (contains access_token).
    """
    return kite.generate_session(request_token, api_secret=API_SECRET)

def set_access_token(token: str):
    kite.set_access_token(token)

def get_ltp(exchange: str, tradingsymbol: str) -> dict:
    key = f"{exchange}:{tradingsymbol}"
    res = kite.ltp(key)
    return res.get(key, {})

def place_market_order(exchange: str, tradingsymbol: str, qty: int,
                       transaction_type: str, product: str = "MIS",
                       simulate: bool = False) -> dict:
    """
    If simulate=True, returns simulated order payload.
    transaction_type: "BUY" or "SELL"
    """
    if simulate:
        return {
            "simulated": True,
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "qty": qty,
            "transaction_type": transaction_type
        }

    prod = getattr(kite, f"PRODUCT_{product}") if hasattr(kite, f"PRODUCT_{product}") else kite.PRODUCT_MIS
    return kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        quantity=qty,
        product=prod,
        order_type=kite.ORDER_TYPE_MARKET
    )
