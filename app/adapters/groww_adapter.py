# app/adapters/groww_adapter.py
import os
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

import httpx
from pydantic import BaseModel, Field, validator

GROWW_BASE = os.getenv("GROWW_BASE_URL", "https://api.groww.in")
GROWW_TOKEN = os.getenv("GROWW_API_TOKEN", "")
REQUEST_TIMEOUT = float(os.getenv("GROWW_REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("GROWW_MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("GROWW_RETRY_BACKOFF", "0.8"))

IST = timezone(timedelta(hours=5, minutes=30))

class OptionLeg(BaseModel):
    strike: float
    expiry: datetime
    option_type: str = Field(..., alias="type")
    oi: int = Field(..., alias="openInterest")
    change_in_oi: Optional[int] = Field(None, alias="changeInOpenInterest")
    ltp: Optional[float] = Field(None, alias="lastTradedPrice")
    volume: Optional[int] = Field(None, alias="volume")
    timestamp: Optional[datetime] = None

    @validator("expiry", pre=True)
    def parse_expiry(cls, v):
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except Exception:
                return datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        return v

    @validator("timestamp", pre=True, always=True)
    def normalize_timestamp_to_ist(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v)
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST)

class OptionChainResponse(BaseModel):
    symbol: str
    timestamp: datetime
    underlying: float
    ce: List[OptionLeg] = []
    pe: List[OptionLeg] = []

    @validator("timestamp", pre=True)
    def parse_ts(cls, v):
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v)
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST)


async def _get_with_retries(url: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> httpx.Response:
    headers = headers or {}
    if GROWW_TOKEN:
        headers.setdefault("Authorization", f"Bearer {GROWW_TOKEN}")
    attempt = 0
    backoff = RETRY_BACKOFF
    last_exc = None
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while attempt < MAX_RETRIES:
            try:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429:
                    await asyncio.sleep(backoff * (attempt + 1))
                    attempt += 1
                    backoff *= 2
                    last_exc = e
                    continue
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exc = e
                await asyncio.sleep(backoff * (attempt + 1))
                attempt += 1
                backoff *= 2
        raise last_exc


async def fetch_option_chain(symbol: str, expiry: Optional[str] = None) -> OptionChainResponse:
    endpoint = f"{GROWW_BASE}/option-chain/{symbol}"
    params = {}
    if expiry:
        params["expiry"] = expiry

    resp = await _get_with_retries(endpoint, params=params)
    data = resp.json()

    normalized = {
        "symbol": data.get("symbol") or symbol,
        "timestamp": data.get("timestamp") or data.get("ts") or datetime.utcnow().isoformat(),
        "underlying": data.get("underlying") or data.get("underlyingValue") or 0.0,
        "ce": data.get("ce", []) or data.get("call", []) or [],
        "pe": data.get("pe", []) or data.get("put", []) or [],
    }
    parsed = OptionChainResponse(**normalized)
    return parsed
