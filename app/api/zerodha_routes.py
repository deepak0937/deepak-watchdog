# app/api/zerodha_routes.py

import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from kiteconnect import KiteConnect
from dotenv import load_dotenv
import redis

load_dotenv()

API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

router = APIRouter(prefix="/zerodha", tags=["Zerodha"])

class RequestToken(BaseModel):
    request_token: str

class OrderParams(BaseModel):
    tradingsymbol: str
    exchange: str = "NSE"
    transaction_type: str  # BUY or SELL
    quantity: int
    order_type: str = "MARKET"
    price: float | None = None

def get_kite():
    access_token = r.get("access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="No access token. Call /zerodha/generate-session first.")
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access_token)
    return kite

@router.post("/generate-session")
def generate_session(req: RequestToken):
    kite = KiteConnect(api_key=API_KEY)
    try:
        data = kite.generate_session(req.request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        r.set("access_token", access_token)
        return {"access_token": access_token, "user": data.get("user_id")}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/positions")
def get_positions():
    kite = get_kite()
    return kite.positions()

@router.get("/orders")
def get_orders():
    kite = get_kite()
    return kite.orders()

@router.post("/place-order")
def place_order(order: OrderParams):
    kite = get_kite()
    try:
        params = order.dict()
        if params["order_type"] == "LIMIT" and not params.get("price"):
            raise HTTPException(status_code=400, detail="Price required for LIMIT order.")
        res = kite.place_order(**params)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
