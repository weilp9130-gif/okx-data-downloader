"""采集入口 API 路由：交易对 / bar 列表"""

from typing import Optional

from fastapi import APIRouter, Request, Query

from ...config.config import Config
from ...db.database import session_scope

router = APIRouter(prefix="/api", tags=["ingestion"])


@router.get("/instruments")
def instruments(
    request: Request,
    inst_type: str = Query("SWAP"),
    keyword: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    """交易对列表（DB 优先，空则 OKX 实时）"""
    from ...db.models import Instrument

    with session_scope() as s:
        q = s.query(Instrument)
        if inst_type:
            q = q.filter(Instrument.inst_type == inst_type)
        if keyword:
            q = q.filter(Instrument.inst_id.ilike(f"%{keyword}%"))
        rows = q.order_by(Instrument.inst_id).limit(limit).all()
        if rows:
            return {"source": "db", "total": len(rows), "items": [r.inst_id for r in rows]}
    # DB 无数据 → OKX 实时拉取（SWAP/USDT/live）
    try:
        from ...client.okx_client import OKXClient

        client = OKXClient()
        data = client.get_instruments(inst_type=inst_type or "SWAP")
        items = [
            d["instId"] for d in data
            if d.get("settleCcy") == "USDT" and d.get("state") == "live"
        ]
        if keyword:
            items = [i for i in items if keyword.lower() in i.lower()]
        return {"source": "okx", "total": len(items), "items": items[:limit]}
    except Exception:
        return {"source": "okx", "total": 0, "items": []}


@router.get("/bars")
def bars(request: Request):
    return {"items": Config().download.kline_bars}
