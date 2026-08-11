"""K线数据下载模块

负责从OKX批量拉取指定交易对、指定时间粒度、指定时间范围的K线数据，
并写入PostgreSQL/TimescaleDB。

支持增量更新、断点续传。
"""

from typing import List, Optional
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import Candle
from database import get_session, session_scope, get_engine
from okx_client import OKXClient
from config import Config
from utils.logger import get_logger
from utils.time_utils import (
    ms_to_datetime,
    parse_date,
    utc_ms_timestamp,
    bar_to_seconds,
)

logger = get_logger(__name__)


class CandleDownloader:
    """K线下载器"""

    # TimescaleDB单表建议批量插入条数
    BULK_SIZE = 500

    def __init__(self, client: OKXClient, cfg: Config = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()
        self.dl_cfg = self.cfg.download

    def download_range(
        self,
        inst_id: str,
        bar: str,
        start: datetime,
        end: datetime,
        overwrite: bool = False,
        force_full_range: bool = False,
    ) -> int:
        """下载指定时间范围内的K线

        通过 start/end 分段分页请求，避免单次请求数量限制。

        Args:
            inst_id: 产品ID
            bar: 时间粒度，如 '1m', '4H'
            start: 开始时间（UTC, naive）
            end: 结束时间（UTC, naive）
            overwrite: 是否覆盖已有的重复数据
            force_full_range: 为 True 时禁用"增量续传"（不根据库内已有
                最大ts 向外推 start），严格按传入的 [start, end] 窗口下载。
                适用于全历史并行下载等需要精确控制窗口边界的场景；
                此时窗口级续传由调用方完成。

        Returns:
            int: 实际写入的K线条数
        """
        # 统一为UTC naive；若未指定start，自动从OKX最早历史(2019年)开始
        if start is None:
            start = datetime(2019, 1, 1)
        start = start.replace(tzinfo=None) if start.tzinfo is not None else start
        end = end.replace(tzinfo=None) if end.tzinfo is not None else end
        if end is None:
            end = datetime.now(timezone.utc).replace(tzinfo=None)

        # 增量续传：若库中已有该合约/粒度数据，则从最大已存ts开始向下游。
        # force_full_range=True 时跳过（由调用方控制窗口），避免干扰精确窗口。
        if not overwrite and not force_full_range:
            max_ts = self._get_max_ts(inst_id, bar)
            if max_ts is not None:
                # 数据库ts带时区，统一为naive UTC
                if max_ts.tzinfo is not None:
                    max_ts = max_ts.astimezone(timezone.utc).replace(tzinfo=None)
                # 从已有最大时间之后开始，避免重复请求历史（重叠部分幂等去重）
                next_ts = max_ts + timedelta(seconds=1)
                if next_ts > start:
                    start = next_ts

        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)

        if end_ms <= start_ms:
            logger.warning(f"结束时间({end})早于/等于开始时间({start})，跳过下载")
            return 0

        logger.info(
            f"开始下载K线: {inst_id} | {bar} | "
            f"{start.isoformat()} ~ {end.isoformat()}"
        )

        # 用 after 分页回溯：第一次取最新，之后 after=本页最老ts 往前翻。
        # 边回溯边分批写库，避免一次性积累海量内存；并定期输出进度日志。
        collected: List[dict] = []
        after_ms = None
        total_fetched = 0
        total_written = 0
        page = 0

        while True:
            page += 1
            raw_candles = self.client.get_candles(
                inst_id=inst_id,
                bar=bar,
                after=after_ms,
                limit=self.dl_cfg.max_candles_per_request,
            )
            if not raw_candles:
                logger.info(f"{inst_id} | {bar} 无可回溯数据，停止")
                break

            # 收集窗口 [start, end] 内的K线
            for c in raw_candles:
                ts_ms = c["ts"]
                if start_ms <= ts_ms <= end_ms:
                    collected.append(c)
                    total_fetched += 1

            # 本页最早(最老)的K线
            oldest_ts = min(c["ts"] for c in raw_candles)
            reached_start = oldest_ts <= start_ms

            # 定时输出进度日志（每5000根或每50页一次）
            if page % 50 == 0 or reached_start or total_fetched >= 5000:
                cur_dt = ms_to_datetime(oldest_ts)
                total_pending = end_ms - start_ms
                progress = (
                    (end_ms - oldest_ts) / total_pending * 100
                    if total_pending > 0 else 0
                )
                progress = max(0.0, min(100.0, progress))
                logger.info(
                    f"[进度] {inst_id} | {bar} | 已拉取 {total_fetched} 根 | "
                    f"回溯至 {cur_dt:%Y-%m-%d %H:%M} | 进度 {progress:.1f}% "
                    f"(页 {page})"
                )
                total_fetched = 0

            # 攒够一批立即写库
            if len(collected) >= self.BULK_SIZE * 4:
                written = self._save_candles(inst_id, bar, collected, overwrite)
                total_written += written
                logger.info(
                    f"  一批已入库 {written} 根（累计 {total_written}），"
                    f"继续回溯至 {ms_to_datetime(oldest_ts):%Y-%m-%d %H:%M}"
                )
                collected.clear()

            if reached_start:
                break

            # 继续往前翻更旧数据
            after_ms = oldest_ts

        # 写库剩余数据
        if collected:
            written = self._save_candles(inst_id, bar, collected, overwrite)
            total_written += written

        logger.info(
            f"K线下载完成: {inst_id} | {bar} | 共 {total_written} 根, "
            f"范围 {start.date()} ~ {end.date()}"
        )
        return total_written

    def _save_candles(
        self,
        inst_id: str,
        bar: str,
        candles: List[dict],
        overwrite: bool = False,
    ) -> int:
        """将原始K线数据批量写入数据库

        使用 PostgreSQL 的 ON CONFLICT DO NOTHING 实现幂等插入，
        利用 (inst_id, bar, ts) 复合主键自动去重，性能远优于逐条查询。

        Args:
            inst_id: 产品ID
            bar: 时间粒度
            candles: K线原始数据列表
            overwrite: 是否覆盖已有数据

        Returns:
            int: 实际写入（新增）的数量
        """
        if not candles:
            return 0

        # 按ts去重（OKX分页可能返回重叠数据）
        dedup = {}
        for c in candles:
            dedup[c["ts"]] = c

        # 批量构造待插入字典
        rows = []
        for ts_ms, c in dedup.items():
            rows.append(
                {
                    "inst_id": inst_id,
                    "bar": bar,
                    "ts": ms_to_datetime(ts_ms),
                    "o": c["o"],
                    "h": c["h"],
                    "l": c["l"],
                    "c": c["c"],
                    "vol": c["vol"],
                    "vol_ccy": c["vol_ccy"],
                    "vol_ccy_quote": c["vol_ccy_quote"],
                    "confirm": c["confirm"],
                }
            )

        if not rows:
            return 0

        stmt = pg_insert(Candle)
        if overwrite:
            # 覆盖模式：冲突则更新价格字段
            stmt = stmt.on_conflict_do_update(
                constraint=Candle.__table__.primary_key,
                set_={
                    "o": stmt.excluded.o,
                    "h": stmt.excluded.h,
                    "l": stmt.excluded.l,
                    "c": stmt.excluded.c,
                    "vol": stmt.excluded.vol,
                    "vol_ccy": stmt.excluded.vol_ccy,
                    "vol_ccy_quote": stmt.excluded.vol_ccy_quote,
                    "confirm": stmt.excluded.confirm,
                },
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                constraint=Candle.__table__.primary_key
            )

        # 分批批量插入，用独立连接避免多线程session冲突
        written = 0
        engine = get_engine()
        with engine.connect() as conn:
            for i in range(0, len(rows), self.BULK_SIZE):
                batch = rows[i:i + self.BULK_SIZE]
                result = conn.execute(stmt, batch)
                written += result.rowcount or 0
            conn.commit()
        return written

    def _get_max_ts(self, inst_id: str, bar: str) -> Optional[datetime]:
        """查询指定合约/粒度已存储的最大K线时间（用于增量续传）"""
        try:
            engine = get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT MAX(ts) FROM candles "
                        "WHERE inst_id = :i AND bar = :b"
                    ),
                    {"i": inst_id, "b": bar},
                ).scalar()
            return row
        except Exception:
            return None

    def update_latest(self, inst_id: str, bar: str, lookback_days: int = 1) -> int:
        """增量更新最近N天的K线数据

        适用于定时任务场景，只拉取最近的数据并补齐。

        Args:
            inst_id: 产品ID
            bar: 时间粒度
            lookback_days: 回看天数

        Returns:
            int: 新写入的K线条数
        """
        from utils.time_utils import utc_now
        now = utc_now()
        start = now - timedelta(days=lookback_days)
        return self.download_range(inst_id=inst_id, bar=bar, start=start, end=now)
