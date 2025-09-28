# routes/oi_routes.py
# -----------------------------------------------
# Public Option OI API (NIFTY/BANKNIFTY/stocks) using NSE F&O bhavcopy archives
# Endpoints:
#   GET /public/oi/daily?symbol=NIFTY&date=YYYY-MM-DD&expiry=YYYY-MM-DD
#   GET /public/oi/history?symbol=NIFTY&start=YYYY-MM-DD&end=YYYY-MM-DD&expiry=YYYY-MM-DD
# -----------------------------------------------

from __future__ import annotations
import io, csv, zipfile, requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict

from fastapi import APIRouter, HTTPException, Query
try:
    # If you want to run this file standalone (uvicorn routes.oi_routes:app), we create a small app too
    from fastapi import FastAPI
    _STANDALONE = True
except Exception:
    _STANDALONE = False

router = APIRouter(prefix="/public", tags=["oi"])

# ---------- HTTP session with retries ----------

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def _session() -> requests.Session:
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=4, read=4, connect=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

# ---------- NSE archive helpers ----------

def _bhav_url_fo(dt: datetime) -> str:
    # Example: https://archives.nseindia.com/content/historical/DERIVATIVES/2025/SEP/fo28SEP2025bhav.csv.zip
    mon = dt.strftime("%b").upper()
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{dt:%Y}/{mon}/fo{dt:%d}{mon}{dt:%Y}bhav.csv.zip"

def _fetch_bhavcopy_fo(dt: datetime) -> List[Dict[str, Any]]:
    sess = _session()
    url = _bhav_url_fo(dt)
    r = sess.get(url, timeout=30)
    if r.status_code == 404:
        raise HTTPException(404, f"NSE FO bhavcopy not found for {dt:%Y-%m-%d} (holiday?)")
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    names = z.namelist()
    if not names:
        raise HTTPException(502, "Empty bhavcopy zip")
    with z.open(names[0]) as f:
        text = io.TextIOWrapper(f, encoding="utf-8")
        reader = csv.DictReader(text)
        return [row for row in reader]

def _parse_float(x) -> float:
    try: return float(x)
    except: return 0.0

def _nearest_or_specific_expiry(opt_rows: List[Dict[str, Any]], wanted_iso: str | None) -> str:
    # opt_rows: rows for a symbol (OPTIDX/OPTSTK)
    # wanted_iso: 'YYYY-MM-DD' or None
    fmt = "%d-%b-%Y"
    expiries = sorted({r["EXPIRY_DT"] for r in opt_rows}, key=lambda d: datetime.strptime(d, fmt))
    if not expiries:
        raise HTTPException(404, "No expiries found in bhavcopy for this symbol")
    if wanted_iso:
        wanted = datetime.strptime(wanted_iso, "%Y-%m-%d").strftime(fmt)
        return wanted if wanted in expiries else expiries[0]
    return expiries[0]

def _build_chain(rows: List[Dict[str, Any]], symbol: str, expiry_iso: str | None) -> Dict[str, Any]:
    # Filter to symbol options
    opt = [r for r in rows if r["SYMBOL"] == symbol and r["INSTRUMENT"] in ("OPTIDX", "OPTSTK")]
    if not opt:
        return {"symbol": symbol, "msg": "no option data", "strikes": []}
    expiry_str = _nearest_or_specific_expiry(opt, expiry_iso)
    chain = [r for r in opt if r["EXPIRY_DT"] == expiry_str]

    book: Dict[float, Dict[str, Dict[str, float]]] = defaultdict(lambda: {"CE": {"oi": 0.0, "coi": 0.0, "vol": 0.0},
                                                                        "PE": {"oi": 0.0, "coi": 0.0, "vol": 0.0}})
    for r in chain:
        side = r["OPTION_TYP"]  # CE/PE
        k = float(r["STRIKE_PR"])
        book[k][side]["oi"]  += _parse_float(r.get("OPEN_INT", 0))
        book[k][side]["coi"] += _parse_float(r.get("CHG_IN_OI", 0))
        book[k][side]["vol"] += _parse_float(r.get("CONTRACTS", 0))

    strikes_sorted = sorted(book.keys())
    out = [{
        "strike": k,
        "CE": {kk: round(v, 2) for kk, v in book[k]["CE"].items()},
        "PE": {kk: round(v, 2) for kk, v in book[k]["PE"].items()},
    } for k in strikes_sorted]

    # Top OI strikes (handy for planning)
    top_calls = sorted(((k, book[k]["CE"]["oi"]) for k in strikes_sorted), key=lambda x: x[1], reverse=True)[:10]
    top_puts  = sorted(((k, book[k]["PE"]["oi"]) for k in strikes_sorted), key=lambda x: x[1], reverse=True)[:10]

    trade_date = chain[0]["TIMESTAMP"] if chain else ""
    return {
        "symbol": symbol,
        "trade_date": trade_date,             # e.g. "28-Sep-2025"
        "expiry": expiry_str,                 # e.g. "02-Oct-2025"
        "top_call_strikes": [k for k, _ in top_calls],
        "top_put_strikes":  [k for k, _ in top_puts],
        "strikes": out,
    }

# ---------- Public endpoints ----------

@router.get("/oi/daily")
def oi_daily(
    symbol: str = Query("NIFTY", description="Index/stock symbol (e.g., NIFTY, BANKNIFTY)"),
    date: str | None = Query(None, description="Trade date YYYY-MM-DD (default: today IST)"),
    expiry: str | None = Query(None, description="Contract expiry YYYY-MM-DD (default: nearest)")
):
    """
    Example:
      /public/oi/daily?symbol=NIFTY&date=2025-09-27
      /public/oi/daily?symbol=BANKNIFTY&date=2025-09-27&expiry=2025-10-02
    """
    try:
        dt = _ist_today() if date is None else datetime.strptime(date, "%Y-%m-%d")
        rows = _fetch_bhavcopy_fo(dt)
        data = _build_chain(rows, symbol.upper(), expiry)
        if not data.get("strikes"):
            raise HTTPException(404, f"No OI data for {symbol} on {dt:%Y-%m-%d}")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"OI fetch failed: {e}")

@router.get("/oi/history")
def oi_history(
    symbol: str = Query("NIFTY"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str   = Query(..., description="End date YYYY-MM-DD"),
    expiry: str | None = Query(None, description="Contract expiry YYYY-MM-DD (optional)")
):
    """
    Returns compact day-wise top OI strikes for quick scans/backtests.
    Example:
      /public/oi/history?symbol=NIFTY&start=2025-09-01&end=2025-09-28
    """
    try:
        d0 = datetime.strptime(start, "%Y-%m-%d")
        d1 = datetime.strptime(end,   "%Y-%m-%d")
        if d1 < d0:
            raise HTTPException(400, "end must be >= start")

        out: List[Dict[str, Any]] = []
        cur = d0
        while cur <= d1:
            try:
                rows = _fetch_bhavcopy_fo(cur)
                data = _build_chain(rows, symbol.upper(), expiry)
                out.append({
                    "date": cur.strftime("%Y-%m-%d"),
                    "expiry": data["expiry"],
                    "top_call_strikes": data["top_call_strikes"],
                    "top_put_strikes":  data["top_put_strikes"],
                })
            except HTTPException as e:
                if e.status_code != 404:
                    # keep going on non-holiday errors
                    pass
            except Exception:
                pass
            cur += timedelta(days=1)

        if not out:
            raise HTTPException(404, "No data available in the given range")
        return {"symbol": symbol.upper(), "from": start, "to": end, "days": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"History fetch failed: {e}")

# ---------- Utilities ----------

def _ist_today() -> datetime:
    # quick IST 'today' approximation without external tz lib
    # (UTC +05:30). Render runs in UTC. This is fine for date selection.
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ---------- Standalone runner (optional) ----------
if _STANDALONE:
    # Allows: uvicorn routes.oi_routes:app --host 0.0.0.0 --port 10000
    app = FastAPI(title="OI Routes")
    app.include_router(router)
