"""Shared, cached health checks for the web control plane."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from ..db.database import get_engine, session_scope

_TTL_SECONDS = 20.0
_lock = threading.Lock()
_cached: tuple[float, dict[str, Any]] | None = None
_size_cached: tuple[float, int] | None = None


def _worker_online(rows: list[dict[str, Any]], now: datetime) -> bool:
    return any(
        row.get("last_heartbeat_at")
        and (now - row["last_heartbeat_at"]).total_seconds() <= 30
        for row in rows
    )


def get_health(store, *, force: bool = False) -> dict[str, Any]:
    """Return one consistent health snapshot without probing OKX on each request."""
    global _cached
    now_mono = time.monotonic()
    with _lock:
        if not force and _cached and now_mono - _cached[0] < _TTL_SECONDS:
            return dict(_cached[1])

    result: dict[str, Any] = {
        "db": False,
        "timescale": False,
        "okx_rest": None,
        "okx_ws": False,
        "worker": False,
        "db_error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with session_scope() as session:
            session.execute(text("SELECT 1"))
            result["db"] = True
            result["timescale"] = bool(session.execute(text(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')"
            )).scalar())
            probe_running = int(session.execute(text(
                "SELECT COUNT(*) FROM metadata.jobs "
                "WHERE task_type='LATENCY_PROBE' AND status IN ('ASSIGNED','RUNNING')"
            )).scalar() or 0)
            ws_connected = int(session.execute(text(
                "SELECT COUNT(*) FROM latency_probe_stats "
                "WHERE metric='ws_connected' AND value > 0"
            )).scalar() or 0)
            result["okx_ws"] = bool(probe_running and ws_connected)
    except Exception as exc:
        result["db_error"] = str(exc)[:200]

    if result["db"]:
        try:
            result["worker"] = _worker_online(
                store.list_workers(), datetime.now(timezone.utc)
            )
        except Exception:
            result["worker"] = False
        try:
            from ..client.okx_client import OKXClient
            result["okx_rest"] = OKXClient().get_server_time() is not None
        except Exception:
            result["okx_rest"] = False

    result["status"] = "ok" if result["db"] and result["worker"] else "degraded"
    with _lock:
        _cached = (time.monotonic(), dict(result))
    return result


def get_database_size() -> dict[str, int | None]:
    """Return physical database size; this includes Timescale chunks and indexes."""
    global _size_cached
    now = time.monotonic()
    with _lock:
        if _size_cached and now - _size_cached[0] < _TTL_SECONDS:
            return {"database_bytes": _size_cached[1]}
    with get_engine().connect() as conn:
        total = int(conn.execute(text(
            "SELECT pg_database_size(current_database())"
        )).scalar() or 0)
    with _lock:
        _size_cached = (time.monotonic(), total)
    return {"database_bytes": total}
