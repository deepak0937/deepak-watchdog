# app/adapters/groww_adapter.py
import os
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

import httpx
from pydantic import BaseModel, Field, validator

# ---------- Config ----------
# Prefer GROW_ACCESS_TOKEN (matches deepak_watchdog.py) but fall back to older var for compatibility
GROWW_BASE = os.getenv("GROWW_BASE_URL", "https://api.groww.in")
GROWW_TOKEN = os.getenv("GROW_ACCESS_TOKEN") or os.getenv("GROWW_API_TOKEN", "")
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
    headers = headers or {}
    # Add Bearer token header if token available
    if GROWW_TOKEN:
        headers.setdefault("Authorization", f"Bearer {GROWW_TOKEN}")
        # some APIs use X-API-KEY or x-api-key — include as fallback
        headers.setdefault("X-API-KEY", GROWW_TOKEN)
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
                # retry on rate limiting
                if status == 429:
                    await asyncio.sleep(backoff * (attempt + 1))
                    attempt += 1
                    backoff *= 2
                    last_exc = e
                    continue
                # For 401/403 we don't retry here - bubble up
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exc = e
                await asyncio.sleep(backoff * (attempt + 1))
                attempt += 1
                backoff *= 2
        raise last_exc

# ---------- Option Chain fetcher ----------
async def fetch_option_chain(symbol: str, expiry: Optional[str] = None) -> OptionChainResponse:
    """
    Try several plausible Groww endpoint paths and return the first successful parsed response.
    """
    candidate_paths = [
        f"{GROWW_BASE}/v1/option-chain/{symbol}",
        f"{GROWW_BASE}/v1/option-chain",
        f"{GROWW_BASE}/option-chain/{symbol}",
        f"{GROWW_BASE}/option-chain",
        f"{GROWW_BASE}/v1/market-data/option-chain/{symbol}",
        f"{GROWW_BASE}/v1/marketdata/option-chain/{symbol}",
        f"{GROWW_BASE}/v2/option-chain/{symbol}",
    ]

    params = {}
    if expiry:
        params["expiry"] = expiry

    last_exc = None
    for url in candidate_paths:
        try:
            resp = await _get_with_retries(url, params=params)
            data = resp.json()

            # heuristic: skip non-dict or obvious error payloads
            if not isinstance(data, dict):
                last_exc = Exception(f"Non-object JSON from {url}")
                continue
            if data.get("error") or (data.get("status") and str(data.get("status")).lower() not in ("ok", "success")):
                last_exc = Exception(f"API error at {url}: {data.get('error') or data.get('status')}")
                continue

            # Normalize fie


