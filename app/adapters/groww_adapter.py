# app/adapters/groww_adapter.py
"""
Groww adapter — async, retries, and normalized OptionChain response.
Uses GROW_ACCESS_TOKEN env var (same name used in deepak_watchdog.py).
"""

import os
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

import httpx
from pydantic import BaseModel, Field, validator

# ---------- Config ----------
GROWW_BASE = os.getenv("GROWW_BASE_URL", "https://api.groww.in")
# NOTE: use GROW_ACCESS_TOKEN to match deepak_watchdog.py
GROW_ACCESS_TOKEN = os.getenv("GROW_ACCESS_TOKEN", "")
REQUEST_TIMEOUT = float(os.getenv("GROWW_REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("GROWW_MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("GROWW_RETRY_BACKOFF", "0.8"))

IST = timezone(timedelta(hours=5, minutes=30))

# ---------- Models ----------
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


# ---------- HTTP helper with retries ----------
async def _get_with_retries(url: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> httpx.Response:
    headers = headers.copy() if headers else {}
    if GROW_ACCESS_TOKEN:
        headers.setdefault("Authorization", f"Bearer {GROW_ACCESS_TOKEN}")
    attempt = 0
    backoff = RETRY_BACKOFF
    last_exc: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while attempt < MAX_RETRIES:
            try:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429:
                    last_exc = e
                    await asyncio.sleep(backoff * (attempt + 1))
                    attempt += 1
                    backoff *= 2
                    continue
                # 401/403 will bubble up (auth issue)
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exc = e
                await asyncio.sleep(backoff * (attempt + 1))
                attempt += 1
                backoff *= 2
        if last_exc:
            raise last_exc
        raise Exception("Unknown _get_with_retries failure")


# ---------- Option Chain fetcher ----------
async def fetch_option_chain(symbol: str, expiry: Optional[str] = None) -> OptionChainResponse:
    candidate_paths = [
        f"{GROWW_BASE}/v1/option-chain/{symbol}",
        f"{GROWW_BASE}/v1/option-chain",
        f"{GROWW_BASE}/option-chain/{symbol}",
        f"{GROWW_BASE}/option-chain",
        f"{GROWW_BASE}/v1/market-data/option-chain/{symbol}",
        f"{GROWW_BASE}/v1/marketdata/option-chain/{symbol}",
        f"{GROWW_BASE}/v2/option-chain/{symbol}",
    ]

    params: Dict[str, Any] = {}
    if expiry:
        params["expiry"] = expiry

    last_exc: Optional[Exception] = None

    for url in candidate_paths:
        try:
            resp = await _get_with_retries(url, params=params)
            data = resp.json()
            if not isinstance(data, dict):
                last_exc = Exception(f"Non-object JSON from {url}")
                continue
            if data.get("error") or (data.get("status") and str(data.get("status")).lower() not in ("ok", "success")):
                last_exc = Exception(f"API error at {url}: {data.get('error') or data.get('status')}")
                continue

            normalized = {
                "symbol": data.get("symbol") or (data.get("meta") or {}).get("symbol") or symbol,
                "timestamp": data.get("timestamp") or data.get("ts") or (data.get("meta") or {}).get("timestamp") or datetime.utcnow(),
                "underlying": data.get("underlying") or data.get("underlyingValue") or data.get("underlying_value") or (data.get("meta") or {}).get("underlying") or 0.0,
                "ce": data.get("ce", []) or data.get("call", []) or data.get("calls", []) or (data.get("payload") or {}).get("ce", []) or [],
                "pe": data.get("pe", []) or data.get("put", []) or data.get("puts", []) or (data.get("payload") or {}).get("pe", []) or [],
            }

            parsed = OptionChainResponse(**normalized)
            return parsed

        except Exception as e:
            last_exc = e
            continue

    if last_exc:
        raise last_exc

    return OptionChainResponse(symbol=symbol, timestamp=datetime.utcnow(), underlying=0.0, ce=[], pe=[])
