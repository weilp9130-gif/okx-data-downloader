"""Open Interest 下载模块

注意：OKX 当前仅提供 `/api/v5/public/open-interest` 单点快照接口，
无历史 OI 查询端点。本下载器获取当前快照并写入 open_interest 表，
用户可通过定时运行（如 cron）积累时序数据。
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import Config
from ..database import get_engine
from ..models import OpenInterest, OISyncState
from ..okx_client import OKXClient
from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime

logger = get_logger(__name__)


class OpenInterestDownloader:
    """Open Interest 下载器（当前快照）"""

    def __init__(self, client: Optional[OKXClient] = None, cfg: Optional[Config] = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()

    def download(self, inst_id: str, bar: str = "current") -> int:
        """下载当前 OI 快照并入库

        Args:
            inst_id: 产品ID，如 BTC-USDT-SWAP
            bar: 数据粒度标记（默认 "current"，表示单点快照）
        Returns:
            int: 写入/更新数量
        """
        raw = self.client.get_open_interest(inst_id=inst_id)
        if not raw:
            logger.warning("OKX 返回空的 open-interest: inst_id=%s", inst_id)
            self._record_error(inst_id, bar)
            return 0

        record = normalize_open_interest(raw[0])
        if record is None:
            logger.warning("OpenInterest 归一化失败: %s", raw[0])
            self._record_error(inst_id, bar)
            return 0

        record["bar"] = bar
        written = self._insert([record])
        self._record_success(inst_id, bar, record["ts"])
        logger.info("OpenInterest 下载完成: %s | oi=%s | ts=%s", inst_id, record["oi"], record["ts"])
        return written

    def _insert(self, rows: list) -> int:
        """批量插入（ON CONFLICT DO NOTHING，幂等）"""
        if not rows:
            return 0

        stmt = pg_insert(OpenInterest).values(rows).on_conflict_do_nothing(
            index_elements=["inst_id", "bar", "ts"]
        )
        engine = get_engine()
        with engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount or 0

    def _record_success(self, inst_id: str, bar: str, latest_ts: datetime) -> None:
        """更新同步状态表"""
        now = datetime.now(timezone.utc)
        row = {
            "inst_id": inst_id,
            "bar": bar,
            "latest_ts": latest_ts,
            "last_success_at": now,
            "error_count": 0,
            "status": "HEALTHY",
            "updated_at": now,
        }
        stmt = pg_insert(OISyncState).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "bar"],
            set_={
                "latest_ts": stmt.excluded.latest_ts,
                "last_success_at": stmt.excluded.last_success_at,
                "error_count": 0,
                "status": "HEALTHY",
                "updated_at": stmt.excluded.updated_at,
            },
        )
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(stmt)

    def _record_error(self, inst_id: str, bar: str) -> None:
        """记录同步错误"""
        now = datetime.now(timezone.utc)
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO oi_sync_state (inst_id, bar, last_error_at, error_count, status, updated_at)
                    VALUES (:inst_id, :bar, :now, 1, 'ERROR', :now)
                    ON CONFLICT (inst_id, bar) DO UPDATE SET
                        last_error_at = EXCLUDED.last_error_at,
                        error_count = oi_sync_state.error_count + 1,
                        status = 'ERROR',
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {"inst_id": inst_id, "bar": bar, "now": now},
            )


def normalize_open_interest(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """归一化 OKX open-interest 单条数据"""
    inst_id = raw.get("instId")
    ts_raw = raw.get("ts")
    if not inst_id or not ts_raw:
        return None

    try:
        ts = ms_to_datetime(int(ts_raw))
    except (ValueError, TypeError):
        return None

    return {
        "inst_id": inst_id,
        "ts": ts,
        "oi": raw.get("oi"),
        "oi_ccy": raw.get("oiCcy"),
        "oi_usd": raw.get("oiUsd"),
        "raw_json": raw,
        "ingested_at": datetime.now(timezone.utc),
    }
