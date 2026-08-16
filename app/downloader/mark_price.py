"""Mark Price 下载模块"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import Config
from ..database import get_engine
from ..models import MarkPrice, MarkPriceSyncState
from ..okx_client import OKXClient
from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime, utc_ms_timestamp

logger = get_logger(__name__)


class MarkPriceDownloader:
    """标记价格 K线下载器"""

    def __init__(self, client: Optional[OKXClient] = None, cfg: Optional[Config] = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()

    def download_range(
        self,
        inst_id: str,
        bar: str = "1D",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> int:
        """下载指定时间范围的标记价格 K线

        Args:
            inst_id: 产品ID，如 BTC-USDT-SWAP
            bar: 时间粒度，默认 1D
            start: 开始时间（UTC，含）
            end: 结束时间（UTC，含）

        Returns:
            int: 写入数量
        """
        now = datetime.now(timezone.utc)
        if end is None:
            end = now
        if start is None:
            start = now.replace(year=now.year - 1)

        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)

        records = []
        after = None
        while True:
            page = self.client.get_mark_price_candles(inst_id=inst_id, bar=bar, after=after, limit=100)
            if not page:
                break

            for item in page:
                ts_ms = int(item["ts"])
                if start_ms <= ts_ms <= end_ms:
                    records.append({
                        "inst_id": inst_id,
                        "bar": bar,
                        "ts": ms_to_datetime(ts_ms),
                        "o": item["o"],
                        "h": item["h"],
                        "l": item["l"],
                        "c": item["c"],
                        "confirm": item.get("confirm"),
                        "source": "REST",
                        "fetched_at": datetime.now(timezone.utc),
                        "raw_json": item,
                        "ingested_at": datetime.now(timezone.utc),
                    })

            oldest_ts = int(page[-1]["ts"])
            if oldest_ts <= start_ms:
                break
            if after == oldest_ts:
                break
            after = oldest_ts

        if not records:
            return 0

        written = self._upsert(records)
        self._update_sync_state(inst_id, bar, records[-1]["ts"], records[0]["ts"])
        logger.info("MarkPrice 下载完成: %s | %s | %d 条", inst_id, bar, written)
        return written

    def _upsert(self, rows: list) -> int:
        if not rows:
            return 0
        stmt = pg_insert(MarkPrice).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "bar", "ts"],
            set_={
                "o": stmt.excluded.o,
                "h": stmt.excluded.h,
                "l": stmt.excluded.l,
                "c": stmt.excluded.c,
                "confirm": stmt.excluded.confirm,
                "source": stmt.excluded.source,
                "fetched_at": stmt.excluded.fetched_at,
                "raw_json": stmt.excluded.raw_json,
                "ingested_at": stmt.excluded.ingested_at,
            },
        )
        engine = get_engine()
        with engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount or 0

    def _update_sync_state(self, inst_id: str, bar: str, latest_ts, earliest_ts) -> None:
        now = datetime.now(timezone.utc)
        row = {
            "inst_id": inst_id,
            "bar": bar,
            "latest_ts": latest_ts,
            "earliest_ts": earliest_ts,
            "last_success_at": now,
            "error_count": 0,
            "status": "HEALTHY",
            "updated_at": now,
        }
        stmt = pg_insert(MarkPriceSyncState).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "bar"],
            set_={
                "latest_ts": stmt.excluded.latest_ts,
                "earliest_ts": stmt.excluded.earliest_ts,
                "last_success_at": stmt.excluded.last_success_at,
                "error_count": 0,
                "status": "HEALTHY",
                "updated_at": stmt.excluded.updated_at,
            },
        )
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(stmt)
