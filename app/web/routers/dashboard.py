"""Dashboard 指标 API 路由"""

from fastapi import APIRouter, Request

from ...db.database import session_scope
from ...services.health import get_database_size, get_health

router = APIRouter(prefix="/api", tags=["dashboard"])

# market 表清单（storage_bytes 统计）
MARKET_TABLES = [
    "candles", "funding_rates", "trades", "mark_prices", "index_prices",
    "open_interest", "open_interest_realtime", "trade_aggregates",
    "order_book_snapshots", "order_book_factors",
]

STATUS_ORDER = {"HEALTHY": 0, "WARNING": 1, "STALE": 2, "ERROR": 3, "NO_DATA": 4}


@router.get("/dashboard")
def dashboard(request: Request):
    from sqlalchemy import text

    result = {
        "assets_count": 0,
        "inst_count": 0,
        "total_rows": 0,
        "storage_bytes": 0,
        "running_tasks": 0,
        "abnormal_assets": 0,
        "latest_candle_ts": None,
        "latency": None,
        "health": {},
        "capacity": {"database_bytes": None},
        "collection_status": {},
    }
    try:
        with session_scope() as s:
            result["assets_count"] = int(s.execute(
                text("SELECT COUNT(*) FROM metadata.data_asset")
            ).scalar() or 0)
            result["total_rows"] = int(s.execute(
                text("SELECT COALESCE(SUM(row_count),0) FROM metadata.data_asset_state")
            ).scalar() or 0)
            result["running_tasks"] = int(s.execute(
                text("SELECT COUNT(*) FROM metadata.jobs WHERE status IN ('ASSIGNED','RUNNING')")
            ).scalar() or 0)
            result["abnormal_assets"] = int(s.execute(
                text("SELECT COUNT(*) FROM metadata.data_asset_state WHERE status IN ('WARNING','STALE','ERROR')")
            ).scalar() or 0)
            result["latest_candle_ts"] = s.execute(
                text("SELECT MAX(ts) FROM candles")
            ).scalar()
            # 采集状态：按 inst_id 聚合最差状态
            rows = s.execute(
                text("""
                    SELECT a.inst_id, st.status
                    FROM metadata.data_asset a
                    JOIN metadata.data_asset_state st ON st.asset_id = a.id
                """)
            ).all()
            worst = {}
            for inst_id, status in rows:
                cur = worst.get(inst_id)
                if cur is None or STATUS_ORDER.get(status, 9) > STATUS_ORDER.get(cur, 9):
                    worst[inst_id] = status
            result["collection_status"] = worst

            # 延迟：最新窗口 p50/p95/p99（corrected_ws_receive_latency）
            try:
                lat = s.execute(
                    text("""
                        SELECT p50_ms, p95_ms, p99_ms FROM latency_summaries
                        WHERE metric = 'corrected_ws_receive_latency'
                        ORDER BY window_start DESC LIMIT 1
                    """)
                ).mappings().one_or_none()
                if lat:
                    result["latency"] = {
                        "p50_ms": lat["p50_ms"],
                        "p95_ms": lat["p95_ms"],
                        "p99_ms": lat["p99_ms"],
                    }
            except Exception:
                pass

            try:
                result["inst_count"] = int(s.execute(
                    text("SELECT COUNT(DISTINCT inst_id) FROM instruments WHERE inst_type='SWAP'")
                ).scalar() or 0)
            except Exception:
                pass

    except Exception:
        # Dashboard remains readable in degraded mode; shared health includes reason.
        pass

    result["health"] = get_health(request.app.state.manager.store)
    try:
        result["capacity"] = get_database_size()
        result["storage_bytes"] = int(result["capacity"]["database_bytes"] or 0)
    except Exception:
        pass

    return result
