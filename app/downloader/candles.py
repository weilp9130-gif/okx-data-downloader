"""K线数据下载模块

负责从OKX批量拉取指定交易对、指定时间粒度、指定时间范围的K线数据，
并写入PostgreSQL/TimescaleDB。

支持增量更新、断点续传。
"""

from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone
import time

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..db.models import Candle
from ..db.database import get_engine
from ..client.okx_client import OKXClient
from ..config.config import Config
from ..utils.logger import get_logger
from .write_buffer import get_write_buffer
from ..utils.time_utils import (
    ms_to_datetime,
    utc_ms_timestamp,
    bar_to_seconds,
)

logger = get_logger(__name__)


class CandleDownloader:
    """K线下载器"""

    # TimescaleDB单表建议批量插入条数
    BULK_SIZE = 500

    # 一天的毫秒数（窗口校验按UTC天粒度）
    DAY_MS = 86_400_000

    # 大粒度bar数据量小且对齐不确定，直接全量幂等拉取，不做按天校验
    _FULL_WINDOW_BARS = {"1W", "1M", "1MUtc"}

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
        list_time: Optional[datetime] = None,
    ) -> int:
        """下载指定时间范围内的缺失K线（先查库，只下载缺失窗口）

        默认（overwrite=False）先查询数据库按天统计已有条数并与理论条数
        比对，只对不完整的窗口发起下载：
        - 已完整的数据完全跳过，避免重复下载
        - 只有缺失窗口（如中断后的尾巴、中途缺口）才会从OKX拉取
        - 验证通过的天会被记录到 download_state 水位线，下次运行跳过

        Args:
            inst_id: 产品ID
            bar: 时间粒度，如 '1m', '4H'
            start: 开始时间（UTC, naive）
            end: 结束时间（UTC, naive）
            overwrite: 是否覆盖已有的重复数据
            list_time: 合约实际上线时间（OKX instruments 的 listTime）。
                用于精确判定"上市首日"的理论条数，避免库内头部被截断时
                该缺口被永久漏检。

        Returns:
            int: 实际写入的K线条数
        """
        # 统一为UTC naive；若未指定start，自动从OKX最早历史(2019年)开始
        if start is None:
            start = datetime(2019, 1, 1)
        if end is None:
            end = datetime.now(timezone.utc).replace(tzinfo=None)
        start = start.replace(tzinfo=None) if start.tzinfo is not None else start
        end = end.replace(tzinfo=None) if end.tzinfo is not None else end

        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)
        list_time_ms = (
            utc_ms_timestamp(list_time) if list_time is not None else None
        )

        if end_ms <= start_ms:
            logger.warning(f"结束时间({end})早于/等于开始时间({start})，跳过下载")
            return 0

        if overwrite:
            windows = [(start_ms, end_ms)]
        else:
            windows = self._find_missing_windows(
                inst_id, bar, start_ms, end_ms, list_time_ms
            )
            if not windows:
                # 推进水位线到end，避免下次重新扫描这段已确认完整的历史
                self._advance_verified(inst_id, bar, [], end_ms)
                logger.info(
                    f"{inst_id} | {bar} 库内数据已完整 "
                    f"({start:%Y-%m-%d} ~ {end:%Y-%m-%d})，无需下载"
                )
                return 0
            total_missing = sum(we - ws for ws, we in windows)
            logger.info(
                f"{inst_id} | {bar} 检测到缺失窗口 {len(windows)} 段，"
                f"合计约 {total_missing // self.DAY_MS} 天，开始补下"
            )

        total_written = 0
        done = 0
        # 从最新窗口往回处理：遇到OKX无数据(合约尚未上线等)即停止，
        # 避免对合约上线前的窗口反复空拉。
        for ws, we in reversed(windows):
            done += 1
            written, had_data = self._fetch_backtrack(
                inst_id, bar, ws, we, overwrite
            )
            if not had_data and we < end_ms:
                break
            total_written += written
            if done % 20 == 0 or done == len(windows):
                logger.info(
                    f"[缺失窗口] {inst_id} | {bar} 处理 {done}/{len(windows)} "
                    f"| 回溯至 {ms_to_datetime(ws):%Y-%m-%d} | "
                    f"累计写入 {total_written} 根"
                )

        # 推进验证水位线：缺失窗口起点之前的所有天已确认完整
        if not overwrite:
            self._advance_verified(inst_id, bar, windows, end_ms)

        logger.info(
            f"K线下载完成: {inst_id} | {bar} | 共写入 {total_written} 根, "
            f"范围 {start.date()} ~ {end.date()}"
        )
        return total_written

    def _find_missing_windows(
        self,
        inst_id: str,
        bar: str,
        start_ms: int,
        end_ms: int,
        list_time_ms: Optional[int] = None,
    ) -> List[Tuple[int, int]]:
        """查询数据库已有数据，返回缺失的时间窗口（升序）

        按UTC天粒度统计库内条数并与理论条数比对，只返回不完整的窗口，
        相邻的缺失天会合并为一段，减少分页重启开销。
        已通过 download_state 水位线验证的区间直接跳过，避免每次全量重扫。
        """
        if bar in self._FULL_WINDOW_BARS:
            return [(start_ms, end_ms)]

        check_from_ms = start_ms
        verified = self._get_verified_upto(inst_id, bar)
        if verified is not None:
            v_ms = utc_ms_timestamp(verified)
            if v_ms >= end_ms:
                return []
            check_from_ms = max(start_ms, v_ms)

        day_counts, min_ts_ms = self._day_counts(
            inst_id, bar, check_from_ms, end_ms
        )

        missing: List[Tuple[int, int]] = []
        cur = None
        day_ms = (check_from_ms // self.DAY_MS) * self.DAY_MS
        while day_ms < end_ms:
            w_end = min(day_ms + self.DAY_MS, end_ms)
            have = day_counts.get(day_ms, 0)
            base = day_ms
            if list_time_ms is not None and day_ms <= list_time_ms < w_end:
                # 合约上市首日：以上线时间为理论起点。即使库内头部被截断
                # （min_ts晚于上线时间），该日 have < need 仍会被检出补齐。
                base = list_time_ms
            elif min_ts_ms is not None and day_ms <= min_ts_ms < w_end:
                # 无listTime时的退化处理：以库内最早ts为理论起点，
                # 避免把"上市日只有部分K线"误判为缺失反复拉取。
                base = min_ts_ms
            need = self._expected_bars(bar, base, w_end)
            if have >= need:
                if cur is not None:
                    missing.append((cur, day_ms))
                    cur = None
            else:
                if cur is None:
                    cur = day_ms
            day_ms += self.DAY_MS
        if cur is not None:
            missing.append((cur, end_ms))
        return missing

    @staticmethod
    def _expected_bars(bar: str, a_ms: int, b_ms: int) -> int:
        """[a_ms, b_ms) 区间内按bar对齐的K线理论条数（精确）"""
        bar_ms = bar_to_seconds(bar) * 1000
        if b_ms <= a_ms:
            return 0
        return (b_ms - 1) // bar_ms - a_ms // bar_ms + 1

    def _day_counts(
        self, inst_id: str, bar: str, start_ms: int, end_ms: int
    ) -> Tuple[dict, Optional[int]]:
        """库内按天统计的K线条数，及范围内的最早ts（单次查询）

        Returns:
            ({当天UTC零点毫秒: 条数}, 最早ts毫秒或None)
        """
        engine = get_engine()
        start_dt = ms_to_datetime(start_ms)
        end_dt = ms_to_datetime(end_ms)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT date_trunc('day', ts) AS d, COUNT(*) AS n, "
                    "MIN(ts) AS m "
                    "FROM candles "
                    "WHERE inst_id = :i AND bar = :b AND ts >= :s AND ts < :e "
                    "GROUP BY d"
                ),
                {"i": inst_id, "b": bar, "s": start_dt, "e": end_dt},
            ).mappings().all()
        counts = {}
        min_ms = None
        for r in rows:
            counts[int(r["d"].timestamp() * 1000)] = r["n"]
            m = r["m"]
            if m is not None:
                m_ms = int(m.timestamp() * 1000)
                if min_ms is None or m_ms < min_ms:
                    min_ms = m_ms
        return counts, min_ms

    # ------------------------------------------------------------------
    # 验证水位线（download_state）：已确认完整的历史区间下次跳过重扫
    # ------------------------------------------------------------------
    def _get_verified_upto(self, inst_id: str, bar: str) -> Optional[datetime]:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT verified_upto FROM download_state "
                        "WHERE inst_id = :i AND bar = :b"
                    ),
                    {"i": inst_id, "b": bar},
                ).scalar()
            return row
        except Exception:
            return None

    def _advance_verified(
        self,
        inst_id: str,
        bar: str,
        windows: List[Tuple[int, int]],
        end_ms: int,
    ) -> None:
        """把验证水位线推进到第一个缺失窗口起点（其之前的天均已确认完整）"""
        upto_ms = end_ms if not windows else windows[0][0]
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO download_state "
                        "(inst_id, bar, verified_upto, updated_at) "
                        "VALUES (:i, :b, :u, :t) "
                        "ON CONFLICT (inst_id, bar) DO UPDATE SET "
                        "verified_upto = EXCLUDED.verified_upto, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    {
                        "i": inst_id,
                        "b": bar,
                        "u": ms_to_datetime(upto_ms),
                        "t": datetime.now(timezone.utc),
                    },
                )
                conn.commit()
        except Exception:
            # 水位线表不可用时不阻塞下载，退化为每次都全量检测
            pass

    def _fetch_backtrack(
        self,
        inst_id: str,
        bar: str,
        start_ms: int,
        end_ms: int,
        overwrite: bool = False,
    ) -> Tuple[int, bool]:
        """从end往回回溯下载 [start_ms, end_ms] 窗口，边拉边分批入库

        Returns:
            (实际写入行数, 该窗口是否从OKX拉到任何数据)
        """
        collected: List[dict] = []
        # 从窗口末端往回翻页（after=end 返回ts<end的最新数据），
        # 避免窗口位于历史中部时先遍历更新的数据
        after_ms = end_ms
        total_written = 0
        had_data = False
        page = 0
        last_progress_log = time.monotonic()

        while True:
            page += 1
            raw_candles = self.client.get_candles(
                inst_id=inst_id,
                bar=bar,
                after=after_ms,
                limit=self.dl_cfg.max_candles_per_request,
            )
            if not raw_candles:
                break
            had_data = True

            oldest_ts = min(c["ts"] for c in raw_candles)
            if after_ms is not None and oldest_ts >= after_ms:
                # 分页无进展，防止死循环
                break

            # 收集窗口 [start_ms, end_ms] 内的K线
            for c in raw_candles:
                ts_ms = c["ts"]
                if start_ms <= ts_ms <= end_ms:
                    collected.append(c)

            # 每30秒打一条进度，避免大合约(数万页)刷屏
            now = time.monotonic()
            if now - last_progress_log >= 30:
                last_progress_log = now
                logger.info(
                    f"[进度] {inst_id} | {bar} | 回溯至 "
                    f"{ms_to_datetime(oldest_ts):%Y-%m-%d %H:%M} | 页 {page}"
                )

            # 攒够一批立即写库，避免一次性积累海量内存。
            # 每批用独立短连接(不持连接)，worker数量不受DB连接池限制
            if len(collected) >= self.BULK_SIZE * 4:
                total_written += self._save_candles(
                    inst_id, bar, collected, overwrite
                )
                collected.clear()

            if oldest_ts <= start_ms:
                break

            # 继续往前翻更旧数据
            after_ms = oldest_ts

        if collected:
            total_written += self._save_candles(
                inst_id, bar, collected, overwrite
            )
        return total_written, had_data

    def _save_candles(
        self,
        inst_id: str,
        bar: str,
        candles: List[dict],
        overwrite: bool = False,
        conn=None,
    ) -> int:
        """将原始K线数据批量写入数据库

        使用 PostgreSQL 的 ON CONFLICT DO NOTHING 实现幂等插入，
        利用 (inst_id, bar, ts) 复合主键自动去重，性能远优于逐条查询。
        传入 conn 时复用该连接（避免每批新建连接），由调用方负责事务边界。

        Args:
            inst_id: 产品ID
            bar: 时间粒度
            candles: K线原始数据列表
            overwrite: 是否覆盖已有数据
            conn: 可复用的数据库连接（可选）

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

        # 分批批量插入；传入conn时复用连接，避免每批新建连接的开销
        written = 0
        if conn is not None:
            for i in range(0, len(rows), self.BULK_SIZE):
                batch = rows[i:i + self.BULK_SIZE]
                result = conn.execute(stmt, batch)
                written += result.rowcount or 0
            conn.commit()
            return written

        # 通过写库缓冲限流写入，避免高并发压垮 PostgreSQL
        return get_write_buffer().put(rows, overwrite=overwrite)

    def missing_windows(
        self,
        inst_id: str,
        bar: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        list_time: Optional[datetime] = None,
    ) -> List[Tuple[int, int]]:
        """计算缺失窗口（毫秒对列表），不做下载

        与 download_range 的窗口检测逻辑一致，供"时间窗并行"使用。
        """
        if start is None:
            start = datetime(2019, 1, 1)
        if end is None:
            end = datetime.now(timezone.utc).replace(tzinfo=None)
        start = start.replace(tzinfo=None) if start.tzinfo is not None else start
        end = end.replace(tzinfo=None) if end.tzinfo is not None else end

        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)
        list_time_ms = utc_ms_timestamp(list_time) if list_time is not None else None

        if end_ms <= start_ms:
            return []
        if bar in self._FULL_WINDOW_BARS:
            return [(start_ms, end_ms)]
        return self._find_missing_windows(
            inst_id, bar, start_ms, end_ms, list_time_ms
        )

    def split_days(self, windows: List[Tuple[int, int]], bar: str) -> List[Tuple[int, int]]:
        """把窗口切成按天对齐的子窗口，提升并行度

        仅对小于 1 天的 bar（1m/5m/...）切分；日线及以上粒度数据量小，
        保持整窗任务，避免周线/月线被天级切片重复拉取。
        """
        try:
            if bar_to_seconds(bar) >= self.DAY_MS / 1000:
                return list(windows)
        except ValueError:
            pass
        chunks: List[Tuple[int, int]] = []
        # 1天一片：只拉被判定为不完整的日期，减少对"几乎完整"日期的冗余拉取；
        # 任务粒度更细，配合窗口级并行能更充分打满IP池。
        for ws, we in windows:
            cur = ws
            while cur < we:
                cend = min(cur + self.DAY_MS, we)
                chunks.append((cur, cend))
                cur = cend
        return chunks

    def fetch_window(
        self, inst_id: str, bar: str, ws_ms: int, we_ms: int,
        overwrite: bool = False,
    ) -> int:
        """下载单个缺失窗口，返回写入行数（线程安全，可多线程并行调用）"""
        written, _ = self._fetch_backtrack(inst_id, bar, ws_ms, we_ms, overwrite)
        return written

    def mark_verified(
        self, inst_id: str, bar: str,
        windows: List[Tuple[int, int]], end_ms: int,
    ) -> None:
        """推进验证水位线到第一个缺失窗口起点"""
        self._advance_verified(inst_id, bar, windows, end_ms)

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
