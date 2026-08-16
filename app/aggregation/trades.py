"""Trade 聚合模块

将 `trades` 表按 1s / 1m 等时间桶聚合成 OHLC，支持迟到数据重新计算，
并通过 watermark / is_final 机制标识窗口最终性。
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..database import get_engine
from ..models import TradeAggregate
from ..utils.logger import get_logger
from ..utils.time_utils import bar_to_seconds

logger = get_logger(__name__)


class TradeAggregator:
    """Trade 聚合器"""

    def __init__(self):
        self.engine = get_engine()

    def aggregate(
        self,
        inst_id: str,
        bar: str,
        start: datetime,
        end: datetime,
        watermark_seconds: int = 60,
    ) -> int:
        """对指定时间范围的 trades 进行聚合并写入 trade_aggregates

        Args:
            inst_id: 产品ID
            bar: 时间粒度，如 1s / 1m
            start: 开始时间
            end: 结束时间
            watermark_seconds: 距离现在超过该秒数的桶才标记为 is_final=1

        Returns:
            int: 写入/更新的聚合桶数量
        """
        interval = bar_to_seconds(bar)
        rows = self._fetch_buckets(inst_id, bar, start, end, interval)
        if not rows:
            logger.info("No trades to aggregate: %s %s", inst_id, bar)
            return 0

        now = datetime.now(timezone.utc)
        watermark = now.timestamp() - watermark_seconds

        for row in rows:
            row["is_final"] = 1 if row["ts"].timestamp() <= watermark else 0
            row["updated_at"] = now
            row["bar"] = bar

        written = self._upsert(rows)
        logger.info(
            "Trade aggregation completed: %s | %s | buckets=%d | is_final_watermark=%ss",
            inst_id, bar, written, watermark_seconds,
        )
        return written

    def _fetch_buckets(
        self, inst_id: str, bar: str, start: datetime, end: datetime, interval: int
    ) -> List[Dict]:
        """按时间桶聚合 trades

        使用 GROUP BY + array_agg 取首/末价，每桶仅返回一行。
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        bucket,
                        (ARRAY_AGG(px ORDER BY ts, trade_id))[1] AS o,
                        MAX(px) AS h,
                        MIN(px) AS l,
                        (ARRAY_AGG(px ORDER BY ts DESC, trade_id DESC))[1] AS c,
                        SUM(sz) AS vol,
                        SUM(CASE WHEN side = 'buy' THEN sz ELSE 0 END) AS vol_buy,
                        SUM(CASE WHEN side = 'sell' THEN sz ELSE 0 END) AS vol_sell,
                        SUM(sz) AS vol_contract,
                        COUNT(*) AS cnt,
                        COUNT(*) FILTER (WHERE side = 'buy') AS cnt_buy,
                        COUNT(*) FILTER (WHERE side = 'sell') AS cnt_sell
                    FROM (
                        SELECT
                            to_timestamp(
                                floor(EXTRACT(EPOCH FROM ts) / :interval)::bigint * :interval
                            ) AS bucket,
                            ts,
                            px,
                            sz,
                            side,
                            trade_id
                        FROM trades
                        WHERE inst_id = :inst_id AND ts BETWEEN :start AND :end
                    ) AS ordered
                    GROUP BY bucket
                    ORDER BY bucket
                    """
                ),
                {
                    "inst_id": inst_id,
                    "start": start,
                    "end": end,
                    "interval": interval,
                },
            ).mappings().all()

        return [
            {
                "inst_id": inst_id,
                "ts": r["bucket"],
                "o": r["o"],
                "h": r["h"],
                "l": r["l"],
                "c": r["c"],
                "vol": r["vol"],
                "vol_buy": r["vol_buy"],
                "vol_sell": r["vol_sell"],
                "vol_contract": r["vol_contract"],
                "cnt": r["cnt"],
                "cnt_buy": r["cnt_buy"],
                "cnt_sell": r["cnt_sell"],
            }
            for r in rows
        ]

    def _upsert(self, rows: List[Dict]) -> int:
        if not rows:
            return 0
        stmt = pg_insert(TradeAggregate).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "bar", "ts"],
            set_={
                "o": stmt.excluded.o,
                "h": stmt.excluded.h,
                "l": stmt.excluded.l,
                "c": stmt.excluded.c,
                "vol": stmt.excluded.vol,
                "vol_buy": stmt.excluded.vol_buy,
                "vol_sell": stmt.excluded.vol_sell,
                "vol_contract": stmt.excluded.vol_contract,
                "cnt": stmt.excluded.cnt,
                "cnt_buy": stmt.excluded.cnt_buy,
                "cnt_sell": stmt.excluded.cnt_sell,
                "is_final": stmt.excluded.is_final,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount or 0

    def recompute_after_recovery(
        self, inst_id: str, bar: str, start: datetime, end: datetime
    ) -> int:
        """Recovery 后重新计算指定范围内的聚合桶（is_final 强制为 0）

        Args:
            inst_id: 产品ID
            bar: 时间粒度
            start: 开始时间
            end: 结束时间

        Returns:
            int: 写入/更新的桶数量
        """
        count = self.aggregate(inst_id, bar, start, end, watermark_seconds=0)
        logger.info("Recomputed trade aggregates after recovery: %s | %s | %d buckets", inst_id, bar, count)
        return count
