"""资金费率下载模块

负责从OKX批量拉取合约产品的历史资金费率数据，
并写入PostgreSQL/TimescaleDB。

资金费率按3个成交阶段/8小时一次收取。
"""

from typing import List, Optional, Tuple
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..db.models import FundingRate
from ..db.database import get_engine
from ..client.okx_client import OKXClient
from ..config.config import Config
from ..utils.logger import get_logger
from ..utils.time_utils import (
    as_naive_utc,
    ms_to_datetime,
    utc_ms_timestamp,
    utc_now,
)

logger = get_logger(__name__)


class FundingRateDownloader:
    """资金费率下载器"""

    BULK_SIZE = 500

    def __init__(self, client: OKXClient, cfg: Config = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()
        self.dl_cfg = self.cfg.download

    def download_range(
        self,
        inst_id: str,
        start: datetime,
        end: datetime,
        overwrite: bool = False,
    ) -> int:
        """下载时间段内的资金费率

        Args:
            inst_id: 合约产品ID（如 ETH-USDT-SWAP）
            start: 开始时间（UTC, naive）
            end: 结束时间（UTC, naive）
            overwrite: 是否覆盖已有数据

        Returns:
            int: 写入数量
        """
        start = as_naive_utc(start)
        end = as_naive_utc(end)
        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)

        # 增量续传：库内头部数据已覆盖 start 时，从最大已存ts之后开始，
        # 避免重复下载已有资金费率；头部有缺口时全量幂等拉取补齐。
        if not overwrite:
            mn, mx = self._get_min_max_ts(inst_id)
            if mx is not None:
                mn_ms = utc_ms_timestamp(mn) if mn else None
                if mn_ms is not None and mn_ms <= start_ms:
                    # 头部已覆盖：从库内最大已存ts之后续传
                    start_ms = utc_ms_timestamp(mx) + 1
                    start = ms_to_datetime(start_ms).replace(tzinfo=None)
                # else: 头部有缺口，全量幂等拉取补齐

        if end_ms <= start_ms:
            logger.info(f"{inst_id} 资金费率已是最新，无需下载")
            return 0

        logger.info(
            f"开始下载资金费率: {inst_id} | "
            f"{start.isoformat()} ~ {end.isoformat()}"
        )

        collected: List[dict] = []
        before_ms = None   # 第一次不传，获取最新数据

        while True:
            raw = self.client.get_funding_rate(
                inst_id=inst_id,
                before=before_ms,
                limit=self.dl_cfg.max_funding_rate_per_request,
            )
            if not raw:
                break

            # 收集窗口 [start, end] 内的数据
            for r in raw:
                ts_ms = int(r["ts"])
                if start_ms <= ts_ms <= end_ms:
                    collected.append(r)

            # 本批最老(最早)的资金费率时间
            oldest_ts = min(int(r["ts"]) for r in raw)
            if oldest_ts <= start_ms:
                break   # 已回溯到窗口起点之前
            before_ms = oldest_ts   # 继续往前翻更早的数据

        written = self._save_funding(inst_id, collected, overwrite)

        logger.info(
            f"资金费率下载完成: {inst_id} | 共 {written} 条, "
            f"覆盖 {start.date()} ~ {end.date()}"
        )
        return written

    def _save_funding(
        self,
        inst_id: str,
        rates: List[dict],
        overwrite: bool = False,
    ) -> int:
        """批量保存资金费率数据到数据库（ON CONFLICT 幂等写入）"""
        if not rates:
            return 0

        # 按ts去重
        dedup = {}
        for r in rates:
            dedup[int(r["ts"])] = r

        rows = []
        for ts_ms, r in dedup.items():
            ft_ms = r["funding_time"]
            rows.append(
                {
                    "inst_id": inst_id,
                    "ts": ms_to_datetime(ts_ms),
                    "funding_rate": r["funding_rate"],
                    "realized_rate": r["realized_rate"],
                    "funding_time": (
                        ms_to_datetime(int(ft_ms)) if ft_ms else None
                    ),
                }
            )

        if not rows:
            return 0

        stmt = pg_insert(FundingRate)
        if overwrite:
            stmt = stmt.on_conflict_do_update(
                constraint=FundingRate.__table__.primary_key,
                set_={
                    "funding_rate": stmt.excluded.funding_rate,
                    "realized_rate": stmt.excluded.realized_rate,
                    "funding_time": stmt.excluded.funding_time,
                },
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                constraint=FundingRate.__table__.primary_key
            )

        written = 0
        engine = get_engine()
        with engine.connect() as conn:
            for i in range(0, len(rows), self.BULK_SIZE):
                batch = rows[i:i + self.BULK_SIZE]
                result = conn.execute(stmt, batch)
                written += result.rowcount or 0
            conn.commit()
        return written

    def _get_min_max_ts(
        self, inst_id: str
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """查询库内该合约资金费率的最小/最大时间（用于增量续传）"""
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT MIN(ts) AS mn, MAX(ts) AS mx "
                    "FROM funding_rates WHERE inst_id = :i"
                ),
                {"i": inst_id},
            ).one()
        return row.mn, row.mx

    def update_latest(self, inst_id: str, lookback_days: int = 7) -> int:
        """增量更新最近N天的资金费率"""
        now = utc_now()
        start = now - timedelta(days=lookback_days)
        return self.download_range(inst_id=inst_id, start=start, end=now)
