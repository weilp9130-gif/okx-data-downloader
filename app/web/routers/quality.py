"""数据质量 API 路由：QUALITY_CHECK 任务 + 最近评分查询"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query

from ...db.database import session_scope
from ..schemas import QualityCheckRequest

router = APIRouter(prefix="/api", tags=["quality"])


@router.post("/quality/check")
def quality_check(req: QualityCheckRequest, request: Request):
    """创建 QUALITY_CHECK 任务（深度校验：duplicates/nulls/regression/跨源+评分）"""
    params = {
        "inst": req.inst_id,
        "bar": req.bar,
        "cross_source": req.cross_source,
    }
    try:
        job = request.app.state.manager.submit("QUALITY_CHECK", params)
    except Exception as e:
        raise HTTPException(400, str(e))
    return job


@router.get("/quality/score")
def quality_score(
    request: Request,
    inst: Optional[str] = Query(None),
    bar: Optional[str] = Query(None),
    dataset: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """最近评分（按 inst/dataset 过滤）"""
    from ...db.models import DataAsset, DataAssetState

    with session_scope() as s:
        q = (
            s.query(DataAsset, DataAssetState)
            .join(DataAssetState, DataAssetState.asset_id == DataAsset.id)
            .filter(DataAssetState.quality_score.isnot(None))
        )
        if inst:
            q = q.filter(DataAsset.inst_id == inst)
        if bar:
            q = q.filter(DataAsset.bar == bar)
        if dataset:
            q = q.filter(DataAsset.dataset == dataset)
        rows = (
            q.order_by(DataAssetState.last_check_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "total": len(rows),
            "items": [
                {
                    "inst_id": a.inst_id,
                    "dataset": a.dataset,
                    "bar": a.bar,
                    "quality_score": float(st.quality_score),
                    "status": st.status,
                    "row_count": st.row_count,
                    "last_check_at": st.last_check_at,
                    "detail": st.detail,
                }
                for a, st in rows
            ],
        }
