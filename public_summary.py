from fastapi import APIRouter, HTTPException, Request
from sqlmodel import Session, select
import datetime as dt
import os

router = APIRouter()
ALLOWED_KEY = os.getenv("AI_PUBLIC_KEY", "")

@router.get("/public/summary")
def public_summary(request: Request):
    # ✅ Require ?key=<AI_PUBLIC_KEY>
    if request.query_params.get("key") != ALLOWED_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    with Session(engine) as sess:
        pos = sess.exec(select(Position)).all()
        margin = sess.exec(select(Margin)).first()
        net_pnl = sum(float(p.pnl or 0) for p in pos)
        open_positions = sum(1 for p in pos if p.quantity or 0 != 0)
        return {
            "timestamp": dt.datetime.now(
                dt.timezone(dt.timedelta(hours=5, minutes=30))
            ).isoformat(),
            "net_pnl": round(net_pnl, 2),
            "open_positions": open_positions,
            "equity": getattr(margin, "equity", 0) or 0,
            "cash": getattr(margin, "available_cash", 0) or 0,
        }

