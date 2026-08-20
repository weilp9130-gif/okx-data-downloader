"""周期 K 线下载基类（Mark Price / Index Price 共用）

两个下载器逻辑完全相同，仅模型与客户端方法不同，抽取出本基类避免重复。
"""

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config.config import Config
from ..db.database import get_engine
from ..client.okx_client import OKXClient
from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime, utc_ms_timestamp

logger = get_logger(__name__)


class PeriodicCandleDownloader:
    """周期 K 线下载基类

    子类需定义：
        model:           ORM 模型（MarkPrice / IndexPrice）
        sync_state_model: 同步状态模型
        client_method:    OKXClient 方法名
        label:            日志标识（如 "MarkPrice"）
    """

    model = None
    sync_state_model = None
    client_method = None
    label = "PeriodicCandle"

    def __init__(self, client: Optional[OKXClient] = None, cfg: Optional[Config] = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()

    def download_range(
        self,
        inst_id: str,
        bar: str = "1D",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        on_progress=None,
    ) -> int:
        """下载指定时间范围的周期 K 线

        Args:
            inst_id: 产品ID / 指数ID
            bar: 时间粒度，默认 1D
            start: 开始时间（UTC，含）
            end: 结束时间（UTC，含）
            on_progress: 可选回调 on_progress(batch_written)

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
            page = self._fetch(inst_id, bar, after, 100)
            if not page:
                break

            fetched_at = datetime.now(timezone.utc)
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
                        "fetched_at": fetched_at,
                        "raw_json": item,
                        "ingested_at": fetched_at,
                    })

            oldest_ts = int(page[-1]["ts"])
            if oldest_ts <= start_ms or after == oldest_ts:
                break
            after = oldest_ts

        if not records:
            return 0

        written = self._upsert(records)
        if on_progress is not None:
            on_progress(written)
        # OKX 返回最新在前：records[0] 最新，records[-1] 最旧
        self._update_sync_state(inst_id, bar, records[0]["ts"], records[-1]["ts"])
        logger.info("%s 下载完成: %s | %s | %d 条", self.label, inst_id, bar, written)
        return written

    def _fetch(self, inst_id: str, bar: str, after: Optional[int], limit: int) -> List[dict]:
        return getattr(self.client, self.client_method)(
            inst_id=inst_id, bar=bar, after=after, limit=limit
        )

    def _upsert(self, rows: list) -> int:
        if not rows:
            return 0
        stmt = pg_insert(self.model).values(rows)
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
        stmt = pg_insert(self.sync_state_model).values(row)
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
