# app/api/groww_routes.py
from fastapi import APIRouter, HTTPException, Query
from app.adapters.groww_adapter import fetch_option_chain

router = APIRouter(prefix="/api/groww", tags=["groww"])

@router.get("/option-chain/{symbol}")
async def get_option_chain(symbol: str, expiry: str = Query(None)):
    try:
        data = await fetch_option_chain(symbol, expiry)
        if (not data.ce) and (not data.pe):
            raise HTTPException(status_code=502, detail="Empty option chain received")
        return {"status": "ok", "data": data.dict()}
    except Exception as e:
        # return safe message for now; we will inspect logs for details if needed
        raise HTTPException(status_code=500, detail=str(e))
