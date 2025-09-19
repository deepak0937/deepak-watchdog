# services/zerodha.py
import os
import logging
from kiteconnect import KiteConnect

# -------- logging --------
logger = logging.getLogger("zerodha_service")

API_KEY = os.getenv("ZERODHA_API_KEY")
API_SECRET = os.getenv("ZERODHA_API_SECRET")
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")

kite = None


def _init_kite():
    """
    Initialize KiteConnect instance if not already created.
    """
    global kite
    if not API_KEY or not API_SECRET:
        logger.error("ZERODHA_API_KEY or ZERODHA_API_SECRET missing in environment")
        raise Exception("ZERODHA_API_KEY / ZERODHA_API_SECRET not set in environment")
    if kite is None:
        kite = KiteConnect(api_key=API_KEY)
        if ACCESS_TOKEN:
            kite.set_access_token(ACCESS_TOKEN)
            logger.info("Zerodha Kite initialized with existing access_token")
        else:
            logger.info("Zerodha Kite initialized without access_token")
    return kite


def get_login_url():
    k = _init_kite()
    url = k.login_url()
    logger.info("Generated Zerodha login URL")
    return url


def generate_session(request_token: str) -> dict:
    """
    Exchange request_token for access_token.
    """
    k = _init_kite()
    data = k.generate_session(request_token, api_secret=API_SECRET)
    token = data.get("access_token")
    if token:
        set_access_token(token)
        logger.info("New Zerodha access_token generated and set")
    return data


def set_access_token(token: str):
    """
    Manually set access_token.
    """
    global ACCESS_TOKEN
    k = _init_kite()
    ACCESS_TOKEN = token
    k.set_access_token(token)
    logger.info("Zerodha access_token updated")


def get_ltp(exchange: str, tradingsymbol: str) -> dict:
    """
    Get last traded price for a symbol.
    """
    k = _init_kite()
    key = f"{exchange}:{tradingsymbol}"
    res = k.ltp(key)
    return res.get(key, {})


def place_market_order(exchange: str, tradingsymbol: str, qty: int,
                       transaction_type: str, product: str = "MIS",
                       simulate: bool = False) -> dict:
    """
    Place a Zerodha market order. If simulate=True, return mock response.
    """
    k = _init_kite()
    if simulate:
        logger.info("Simulated market order: %s %s x%s", transaction_type, tradingsymbol, qty)
        return {
            "simulated": True,
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "qty": qty,
            "transaction_type": transaction_type,
            "product": product
        }

    order = k.place_order(
        variety=k.VARIETY_REGULAR,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        quantity=qty,
        product=product,  # Zerodha expects string "MIS"/"CNC"
        order_type=k.ORDER_TYPE_MARKET
    )
    logger.info("Placed market order: %s", order)
    return order

