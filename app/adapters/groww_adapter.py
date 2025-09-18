# app/adapters/groww_adapter.py
"""
Groww adapter — safe, async, retrying HTTP client and normalized OptionChain response.
Replace your existing app/adapters/groww_adapter.py with this file.
"""

import os
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

import httpx
from pydantic import BaseModel, Field, validator

# ---------- Config ----------
GROWW_BASE = os.getenv("GROWW_BASE_URL", "https://api.groww.in")
GROWW_TOKEN = os.getenv("GROWW_API_TOKEN", "")  # must match Render env var
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
        # ensure tz-aware and convert to IST
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
    """
    Async GET with retry/backoff. Raises the last exception on failure.
    """
    headers = headers.copy() if headers else {}
    if GROWW_TOKEN:
        headers.setdefault("Authorization", f"Bearer {GROWW_TOKEN}")
    attempt = 0
    backoff = RETRY_BACKOFF
    last_exc: Optional[Exception] = None

    # Use a single AsyncClient per call to avoid connection issues in ephemeral contexts
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while attempt < MAX_RETRIES:
            try:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                # on rate limit try backoff; on 401/403 just raise immediately (auth issue)
                if status == 429:
                    last_exc = e
                    await asyncio.sleep(backoff * (attempt + 1))
                    attempt += 1
                    backoff *= 2
                    continue
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exc = e
                await asyncio.sleep(backoff * (attempt + 1))
                attempt += 1
                backoff *= 2
        # exhausted retries -> re-raise last captured exception
        if last_exc:
            raise last_exc
        raise Exception("Unknown _get_with_retries failure")


# ---------- Option Chain fetcher ----------
async def fetch_option_chain(symbol: str, expiry: Optional[str] = None) -> OptionChainResponse:
    """
    Try several plausible Groww endpoint paths and return the first successful parsed response.
    Raises an exception if none of the endpoints worked.
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

    params: Dict[str, Any] = {}
    if expiry:
        params["expiry"] = expiry

    last_exc: Optional[Exception] = None

    for url in candidate_paths:
        try:
            resp = await _get_with_retries(url, params=params)
            # safe .json() call — httpx.Response.json can raise; let it propagate to be caught below
            data = resp.json()

            # heuristic: require object/dict
            if not isinstance(data, dict):
                last_exc = Exception(f"Non-object JSON from {url}")
                continue

            # skip obvious error payloads
            if data.get("error") or (data.get("status") and str(data.get("status")).lower() not in ("ok", "success")):
                last_exc = Exception(f"API error at {url}: {data.get('error') or data.get('status')}")
                continue

            # Normalize common field names coming from various Groww payload shapes
            normalized = {
                "symbol": data.get("symbol") or (data.get("meta") or {}).get("symbol") or symbol,
                "timestamp": data.get("timestamp") or data.get("ts") or (data.get("meta") or {}).get("timestamp") or datetime.utcnow(),
                "underlying": data.get("underlying") or data.get("underlyingValue") or data.get("underlying_value") or (data.get("meta") or {}).get("underlying") or 0.0,
                "ce": data.get("ce", []) or data.get("call", []) or data.get("calls", []) or (data.get("payload") or {}).get("ce", []) or [],
                "pe": data.get("pe", []) or data.get("put", []) or data.get("puts", []) or (data.get("payload") or {}).get("pe", []) or [],
            }

            # Pydantic will coerce and validate
            parsed = OptionChainResponse(**normalized)
            return parsed

        except Exception as e:
            # store last exception and continue trying next URL
            last_exc = e
            continue

    # If we reach here, nothing worked
    if last_exc:
        raise last_exc

    # fallback (shouldn't happen): return an empty parsed structure
    return OptionChainResponse(
        symbol=symbol,
        timestamp=datetime.utcnow(),
        underlying=0.0,
        ce=[],
        pe=[]
    )
