"""实时监控 API 路由：系统健康 / Worker 列表 / 延迟窗口 / 实时样本 / 重连统计"""

from typing import Optional

from fastapi import APIRouter, Request, Query

from ...db.database import session_scope
from ...services.health import get_health

router = APIRouter(prefix="/api", tags=["monitor"])


@router.get("/monitor/system")
def system_health(request: Request):
    """系统健康（DB / Worker / OKX）"""
    return get_health(request.app.state.manager.store)


@router.get("/monitor/workers")
def workers(request: Request):
    from datetime import datetime, timezone

    try:
        rows = request.app.state.manager.store.list_workers()
    except Exception:
        return {"items": []}
    now = datetime.now(timezone.utc)
    for w in rows:
        hb = w.get("last_heartbeat_at")
        w["online"] = bool(hb and (now - hb).total_seconds() <= 30)
    return {"items": rows}


@router.get("/monitor/latency/summary")
def latency_summary(
    request: Request,
    inst_id: Optional[str] = Query(None),
    channel: Optional[str] = Query(None),
    metric: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=168),
):
    """延迟窗口汇总（latency_summaries 最新窗口）"""
    from sqlalchemy import text

    sql = """
        SELECT window_start, source, inst_id, channel, metric, n,
               p50_ms, p95_ms, p99_ms, max_ms, jitter_ms
        FROM latency_summaries
        WHERE window_start >= now() - (:hours || ' hours')::interval
    """
    params = {"hours": hours}
    if inst_id:
        sql += " AND inst_id = :inst_id"
        params["inst_id"] = inst_id
    if channel:
        sql += " AND channel = :channel"
        params["channel"] = channel
    if metric:
        sql += " AND metric = :metric"
        params["metric"] = metric
    sql += " ORDER BY window_start DESC LIMIT 500"
    with session_scope() as s:
        rows = s.execute(text(sql), params).mappings().all()
        return [
            {
                "window_start": r["window_start"],
                "source": r["source"],
                "inst_id": r["inst_id"],
                "channel": r["channel"],
                "metric": r["metric"],
                "n": r["n"],
                "p50_ms": r["p50_ms"],
                "p95_ms": r["p95_ms"],
                "p99_ms": r["p99_ms"],
                "max_ms": r["max_ms"],
                "jitter_ms": r["jitter_ms"],
            }
            for r in rows
        ]


@router.get("/monitor/latency/live")
def latency_live(
    request: Request,
    inst_id: Optional[str] = Query(None),
    channel: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    """实时样本（latency_samples 最近）"""
    from sqlalchemy import text

    sql = """
        SELECT sample_ts, session, source, inst_id, channel, metric,
               value_ms, exchange_ts, recv_ts
        FROM latency_samples
    """
    conds = []
    params = {}
    if inst_id:
        conds.append("inst_id = :inst_id")
        params["inst_id"] = inst_id
    if channel:
        conds.append("channel = :channel")
        params["channel"] = channel
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY sample_ts DESC LIMIT :limit"
    params["limit"] = limit
    with session_scope() as s:
        rows = s.execute(text(sql), params).mappings().all()
        return [
            {
                "sample_ts": r["sample_ts"],
                "session": r["session"],
                "inst_id": r["inst_id"],
                "channel": r["channel"],
                "metric": r["metric"],
                "value_ms": r["value_ms"],
            }
            for r in rows
        ]


@router.get("/monitor/latency/stats")
def latency_stats(
    request: Request,
    hours: int = Query(24, ge=1, le=168),
):
    """重连/丢包统计（latency_probe_stats）"""
    from sqlalchemy import text

    sql = """
        SELECT window_start, source, metric, value
        FROM latency_probe_stats
        WHERE window_start >= now() - (:hours || ' hours')::interval
        ORDER BY window_start DESC, metric
    """
    with session_scope() as s:
        rows = s.execute(text(sql), {"hours": hours}).mappings().all()
        return [
            {
                "window_start": r["window_start"],
                "source": r["source"],
                "metric": r["metric"],
                "value": r["value"],
            }
            for r in rows
        ]
