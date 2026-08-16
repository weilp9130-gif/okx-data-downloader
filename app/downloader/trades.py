"""Trades 历史成交下载模块"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import Config
from ..conflict import DataConflictDetector, canonical_hash
from ..database import get_engine
from ..models import Trade, TradesSyncState
from ..okx_client import OKXClient
from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime

logger = get_logger(__name__)


class TradesDownloader:
    """Trades 下载器"""

    def __init__(self, client: Optional[OKXClient] = None, cfg: Optional[Config] = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()
        self.conflicts = DataConflictDetector()

    def download_range(
        self,
        inst_id: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 100,
        max_pages: int = 10,
        after_trade_id: Optional[str] = None,
    ) -> int:
        """下载指定时间范围的 trades

        Args:
            inst_id: 产品ID，如 BTC-USDT-SWAP
            start: 开始时间（UTC），默认一年前
            end: 结束时间（UTC），默认当前
            limit: 每页数量
            max_pages: 最大分页数
            after_trade_id: 从此 tradeId 之后开始获取（用于断线恢复）

        Returns:
            int: 写入数量
        """
        now = datetime.now(timezone.utc)
        if end is None:
            end = now
        if start is None:
            start = now.replace(year=now.year - 1)

        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        records: List[Dict] = []
        after = after_trade_id
        page_count = 0
        duplicate_count = 0
        first_trade_id = None
        last_trade_id = None
        first_ts = None
        last_ts = None

        while page_count < max_pages:
            page = self.client.get_history_trades(inst_id=inst_id, after=after, limit=limit)
            page_count += 1
            if not page:
                break

            seen_in_page = set()
            for raw in page:
                trade_id = raw.get("tradeId")
                ts_raw = raw.get("ts")
                if trade_id in seen_in_page:
                    duplicate_count += 1
                    continue
                seen_in_page.add(trade_id)

                ts_ms = int(ts_raw)
                # 只保留时间窗口内的数据；但分页仍需用 tradeId 继续
                if start_ms <= ts_ms <= end_ms:
                    records.append(self._normalize(raw, inst_id))

                if first_trade_id is None:
                    first_trade_id = trade_id
                    first_ts = ts_ms
                last_trade_id = trade_id
                last_ts = ts_ms

            oldest_trade_id = page[-1].get("tradeId")
            oldest_ts = int(page[-1].get("ts"))
            if oldest_ts <= start_ms:
                break
            if after == oldest_trade_id:
                logger.warning("Trades 分页 stuck: after=%s", after)
                break
            after = oldest_trade_id

        written = self._insert(records)
        self._update_sync_state(inst_id, last_trade_id, last_ts)

        logger.info(
            "Trades 下载完成: %s | pages=%d | duplicates=%d | first=%s | last=%s | written=%d",
            inst_id, page_count, duplicate_count, first_trade_id, last_trade_id, written,
        )
        return written

    def _normalize(self, raw: dict, inst_id: str) -> dict:
        ts = ms_to_datetime(int(raw["ts"]))
        raw_json = raw
        raw_hash = canonical_hash(raw_json)
        return {
            "inst_id": inst_id,
            "trade_id": raw["tradeId"],
            "ts": ts,
            "px": raw["px"],
            "sz": raw["sz"],
            "side": raw["side"],
            "source": "REST",
            "fetched_at": datetime.now(timezone.utc),
            "ingested_at": datetime.now(timezone.utc),
            "fill_time": ts,
            "raw_json": raw_json,
            "raw_hash": raw_hash,
        }

    BULK_SIZE = 200

    def _insert(self, rows: List[dict]) -> int:
        if not rows:
            return 0
        # Raw 数据不可静默覆盖：同键不同 payload 登记 DATA_CONFLICT 后剔除
        safe_rows, conflicts = self.conflicts.detect_trades(rows)
        if conflicts:
            logger.error(
                "检测到 %d 条 trades DATA_CONFLICT，已登记并跳过写入", len(conflicts)
            )
        written = 0
        for i in range(0, len(safe_rows), self.BULK_SIZE):
            batch = safe_rows[i : i + self.BULK_SIZE]
            written += self._insert_batch(batch)
        return written

    def _insert_batch(self, rows: List[dict]) -> int:
        if not rows:
            return 0
        stmt = pg_insert(Trade).values(rows)
        # hash 相同的重复数据无需覆盖（Raw 不可变）
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["inst_id", "trade_id", "ts"]
        )
        engine = get_engine()
        with engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount or 0

    def _update_sync_state(self, inst_id: str, latest_trade_id, latest_ts_ms) -> None:
        now = datetime.now(timezone.utc)
        latest_ts = ms_to_datetime(latest_ts_ms) if latest_ts_ms else None
        row = {
            "inst_id": inst_id,
            "latest_trade_id": latest_trade_id,
            "latest_ts": latest_ts,
            "status": "HEALTHY",
            "error_count": 0,
            "updated_at": now,
        }
        stmt = pg_insert(TradesSyncState).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id"],
            set_={
                "latest_trade_id": stmt.excluded.latest_trade_id,
                "latest_ts": stmt.excluded.latest_ts,
                "status": "HEALTHY",
                "error_count": 0,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(stmt)
