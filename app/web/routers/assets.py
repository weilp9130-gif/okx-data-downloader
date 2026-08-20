"""资产 API 路由：定义 / 树 / 列表 / 详情 / 刷新"""

from typing import Optional

from sqlalchemy import func

from fastapi import APIRouter, HTTPException, Request, Query

from ...db.database import session_scope
from ...utils.logger import get_logger
from ..schemas import AssetRefreshRequest

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["assets"])


@router.get("/assets/definitions")
def definitions(request: Request):
    from ...db.models import DatasetDefinition

    with session_scope() as s:
        rows = s.query(DatasetDefinition).all()
        return [
            {
                "dataset": r.dataset,
                "bar": r.bar,
                "version": r.version,
                "table_name": r.table_name,
                "primary_time_column": r.primary_time_column,
                "interval_seconds": r.interval_seconds,
                "expected_freshness_sec": r.expected_freshness_sec,
                "retention_days": r.retention_days,
                "enabled": r.enabled,
            }
            for r in rows
        ]


def _assets_query(inst_id: Optional[str] = None, dataset: Optional[str] = None):
    """DataAsset + DataAssetState 关联查询（SQLAlchemy ORM）"""
    from ...db.models import DataAsset, DataAssetState

    with session_scope() as s:
        q = (
            s.query(DataAsset, DataAssetState)
            .outerjoin(DataAssetState, DataAssetState.asset_id == DataAsset.id)
        )
        if inst_id:
            q = q.filter(DataAsset.inst_id == inst_id)
        if dataset:
            q = q.filter(DataAsset.dataset == dataset)
        rows = q.order_by(DataAsset.inst_id, DataAsset.dataset, DataAsset.bar).all()
        return [
            {
                "id": a.id,
                "exchange": a.exchange,
                "market": a.market,
                "inst_id": a.inst_id,
                "dataset": a.dataset,
                "bar": a.bar,
                "state": _state_to_dict(st),
            }
            for a, st in rows
        ]


def _state_to_dict(st) -> Optional[dict]:
    if st is None:
        return None
    return {
        "earliest_ts": st.earliest_ts,
        "latest_ts": st.latest_ts,
        "row_count": st.row_count,
        "expected_rows": st.expected_rows,
        "missing_rows": st.missing_rows,
        "duplicates": st.duplicates,
        "invalid_rows": st.invalid_rows,
        "quality_score": float(st.quality_score) if st.quality_score is not None else None,
        "freshness_lag_sec": st.freshness_lag_sec,
        "status": st.status,
        "checked_at": st.checked_at,
        "full_recount_at": st.full_recount_at,
        "last_check_at": st.last_check_at,
        "detail": st.detail,
    }


@router.get("/assets/tree")
def assets_tree(request: Request):
    assets = _assets_query()
    tree = {}
    for a in assets:
        node = tree.setdefault(a["inst_id"], {"inst_id": a["inst_id"], "datasets": {}})
        node["datasets"][a["dataset"] + (f"/{a['bar']}" if a["bar"] else "")] = {
            "asset_id": a["id"],
            "dataset": a["dataset"],
            "bar": a["bar"],
            "status": (a["state"] or {}).get("status", "NO_DATA"),
            "row_count": (a["state"] or {}).get("row_count", 0),
            "quality_score": (a["state"] or {}).get("quality_score"),
        }
    return list(tree.values())


@router.get("/assets/instruments")
def list_asset_instruments(
    request: Request,
    keyword: Optional[str] = Query(None, max_length=64),
    status: Optional[str] = Query(None, max_length=20),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Paginated instrument summaries for the web UI.

    The legacy tree endpoint intentionally remains available for integrations,
    while the UI loads datasets only after an instrument is selected.
    """
    from ...db.models import DataAsset, DataAssetState

    with session_scope() as s:
        q = (
            s.query(
                DataAsset.inst_id.label("inst_id"),
                func.count(DataAsset.id).label("dataset_count"),
                func.coalesce(func.sum(DataAssetState.row_count), 0).label("row_count"),
                func.max(DataAssetState.status).label("status"),
            )
            .outerjoin(DataAssetState, DataAssetState.asset_id == DataAsset.id)
        )
        if keyword:
            q = q.filter(DataAsset.inst_id.ilike(f"%{keyword}%"))
        if status:
            q = q.filter(DataAssetState.status == status)
        q = q.group_by(DataAsset.inst_id)
        total = q.count()
        rows = q.order_by(DataAsset.inst_id).offset(offset).limit(limit).all()
    return {
        "total": total,
        "items": [
            {
                "inst_id": row.inst_id,
                "dataset_count": int(row.dataset_count or 0),
                "row_count": int(row.row_count or 0),
                "status": row.status or "NO_DATA",
            }
            for row in rows
        ],
    }


@router.get("/assets")
def list_assets(
    request: Request,
    inst_id: Optional[str] = Query(None),
    dataset: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    assets = _assets_query(inst_id=inst_id, dataset=dataset)
    if status:
        assets = [a for a in assets if (a["state"] or {}).get("status") == status]
    return {"total": len(assets), "items": assets[:limit]}


@router.get("/assets/{asset_id}")
def asset_detail(asset_id: int, request: Request):
    from ...db.models import DataAsset

    with session_scope() as s:
        a = s.query(DataAsset).filter(DataAsset.id == asset_id).first()
        if a is None:
            raise HTTPException(404, f"资产不存在: {asset_id}")
    item = {
        "id": a.id,
        "exchange": a.exchange,
        "market": a.market,
        "inst_id": a.inst_id,
        "dataset": a.dataset,
        "bar": a.bar,
        "created_at": a.created_at,
        "updated_at": a.updated_at,
    }
    # 单独查询 state（避免懒加载会话关闭问题）
    from ...db.models import DataAssetState

    with session_scope() as s:
        st = s.query(DataAssetState).filter(DataAssetState.asset_id == asset_id).first()
        item["state"] = _state_to_dict(st)
    return item


@router.post("/assets/refresh")
def refresh_assets(req: AssetRefreshRequest, request: Request):
    """创建 ASSET_REFRESH 任务（all/incremental 为主，full 兜底）"""
    params = {
        "scope": req.scope,
        "mode": req.mode,
    }
    if req.inst_id:
        params["scope"] = "inst"
        params["inst_id"] = req.inst_id
    try:
        job = request.app.state.manager.submit("ASSET_REFRESH", params)
    except Exception as e:
        raise HTTPException(400, str(e))
    return job


@router.post("/assets/{asset_id}/refresh")
def refresh_single_asset(asset_id: int, request: Request):
    """单资产浅刷新（同步执行，秒级）"""
    from ...db.models import DataAsset
    from ...services.assets import refresh_asset

    with session_scope() as s:
        a = s.query(DataAsset).filter(DataAsset.id == asset_id).first()
    if a is None:
        raise HTTPException(404, f"资产不存在: {asset_id}")
    try:
        summary = refresh_asset(a.inst_id, a.dataset, a.bar, mode="incremental")
    except Exception as e:
        raise HTTPException(500, str(e))
    return summary
