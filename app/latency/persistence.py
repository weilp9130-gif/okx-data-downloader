"""延迟探针持久化（persistence.py）

- LatencySampleWriter(BaseWriter)：flush 状态机
  `parsed -> queued -> flush attempt -> success(written) / fail(write_errors
  按 attempt) -> retry -> final_write_failed_samples`；DB 失败绝不入 dropped。
- written 按 sample_ts 窗口归属（跨窗口批次拆分）；write_errors 按错误发生
  时间窗口、每次 attempt +1（P1-21 written 最终一致：writer drain/停止前已
  关闭窗口仍可收到迟到 written 更新，直接 UPSERT）。
- market 计数器范围（P0-11）：written 只计 market 行。
- summaries/stats UPSERT（内存聚合结果；禁 SQL 反查 samples）。
"""

import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..db.database import get_engine
from ..db.models import LatencyProbeStats, LatencySample, LatencySummary
from ..realtime.writer import BaseWriter
from ..utils.logger import get_logger
from .metrics import (
    CORRECTED_METRIC,
    MAX_CHANNEL_LEN,
    MAX_INST_ID_LEN,
    MAX_METRIC_LEN,
    MAX_SOURCE_LEN,
    SAMPLE_CLASS_MARKET,
    SOURCE_OKX,
    STAT_DROPPED,
    STAT_FINAL_FAILED,
    STAT_QUEUED,
    STAT_SAMPLES_PARSED,
    STAT_WRITE_ERRORS,
    STAT_WRITTEN,
    window_start_for,
)

logger = get_logger(__name__)

# written 累计簿记的裁剪窗口（秒）：足够覆盖任何现实写入延迟，且限制内存有界
WRITTEN_PRUNE_HORIZON = 2 * 3600


def _summary_upsert_stmt(rows: List[dict]):
    stmt = pg_insert(LatencySummary).values(rows)
    return stmt.on_conflict_do_update(
        index_elements=["window_start", "source", "channel", "metric", "inst_id"],
        set_={
            "n": stmt.excluded.n,
            "min_ms": stmt.excluded.min_ms,
            "mean_ms": stmt.excluded.mean_ms,
            "p50_ms": stmt.excluded.p50_ms,
            "p95_ms": stmt.excluded.p95_ms,
            "p99_ms": stmt.excluded.p99_ms,
            "max_ms": stmt.excluded.max_ms,
            "jitter_ms": stmt.excluded.jitter_ms,
        },
    )


def _stats_upsert_stmt(rows: List[dict]):
    stmt = pg_insert(LatencyProbeStats).values(rows)
    return stmt.on_conflict_do_update(
        index_elements=["window_start", "source", "metric"],
        set_={"value": stmt.excluded.value},
    )


def upsert_summaries(engine, rows: List[dict]) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(_summary_upsert_stmt(rows))


def upsert_stats(engine, rows: List[dict]) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(_stats_upsert_stmt(rows))


def market_written_by_window(batch: List[dict], summary_interval: int) -> Dict[datetime, int]:
    """只统计 market 行的 written，按每行 sample_ts 窗口归属（P0-11）。"""
    out: Dict[datetime, int] = {}
    for item in batch:
        if item.get("sample_class") != SAMPLE_CLASS_MARKET:
            continue
        w = window_start_for(item["sample_ts"], summary_interval)
        out[w] = out.get(w, 0) + 1
    return out


class LatencySampleWriter(BaseWriter):
    """延迟样本写入器：flush 状态机 + market 计数器 + written 最终一致。"""

    MAX_BUFFER_SIZE = 100000
    FLUSH_INTERVAL = 1.0
    BATCH_SIZE = 5000
    PUT_TIMEOUT = 0.2
    RETRY_BACKOFF = 1.0
    MAX_RETRY_BACKOFF = 30.0

    def __init__(self, stats, summary_interval: int, engine=None, source: str = SOURCE_OKX):
        self.stats = stats
        self.summary_interval = int(summary_interval)
        self.source = source
        self.engine = engine or get_engine()
        self._committed_written: Dict[datetime, int] = {}   # 累计 committed written（按窗口）
        self._final_failed_rows: List[dict] = []
        self._final_failed_reported = False
        # C-P0-5：保护 _pending / _final_failed_rows / _committed_written（RLock 可重入）
        self._state_lock = threading.RLock()
        super().__init__()

    # ------------------------------------------------------------------
    def queue_depth(self) -> int:
        return self._buffer.qsize()

    def queue_capacity(self) -> int:
        return self.MAX_BUFFER_SIZE

    # ------------------------------------------------------------------
    def stop(self, timeout: float = 5.0) -> None:
        self._stopped.set()
        self._thread.join(timeout=timeout)
        thread_alive = self._thread.is_alive()
        if thread_alive:
            logger.error(
                "Writer thread did not stop within %.1fs; reconciling remaining "
                "rows best-effort (no silent loss allowed)", timeout,
            )
        # C-P0-5：join 后（无论是否存活）收集 buffer + _pending 剩余行，尝试 flush
        remaining: List[dict] = []
        with self._state_lock:
            pending = list(self._pending)
            self._pending = []
        while True:
            try:
                remaining.append(self._buffer.get_nowait())
            except queue.Empty:
                break
        remaining = pending + remaining
        if remaining:
            try:
                self._flush(remaining)
            except Exception as e:
                logger.error("Final buffer drain flush failed: %s", e)
                with self._state_lock:
                    self._final_failed_rows.extend(remaining)
        if thread_alive:
            logger.error(
                "Writer thread still alive after stop: rows in its in-flight batch "
                "may be unreconciled; accounting below is best-effort"
            )
        self._report_final_failed()
        self._log_accounting()

    def _log_accounting(self) -> None:
        """停止时打印对账（C-P0-5）：samples_parsed == queued + dropped 且
        queued == written + final_failed（以实际统计语义为准；任何无法解释的
        sample 丢失都视为 bug）。"""
        parsed = self.stats.total(STAT_SAMPLES_PARSED)
        queued = self.stats.total(STAT_QUEUED)
        dropped = self.stats.total(STAT_DROPPED)
        written = self.stats.total(STAT_WRITTEN)
        final_failed = self.stats.total(STAT_FINAL_FAILED)
        if parsed != queued + dropped:
            logger.error(
                "ACCOUNTING MISMATCH samples_parsed=%d != queued=%d + dropped=%d",
                parsed, queued, dropped,
            )
        else:
            logger.info("ACCOUNTING samples_parsed=%d == queued=%d + dropped=%d",
                        parsed, queued, dropped)
        if queued != written + final_failed:
            logger.error(
                "ACCOUNTING MISMATCH queued=%d != written=%d + final_failed=%d",
                queued, written, final_failed,
            )
        else:
            logger.info("ACCOUNTING queued=%d == written=%d + final_failed=%d",
                        queued, written, final_failed)

    def _run(self) -> None:
        """后台写入线程（override）：停止时 final flush，失败行计入
        final_write_failed_samples 并显式报告。"""
        batch: List[dict] = []
        backoff = self.RETRY_BACKOFF
        final_failed: List[dict] = []
        while not self._stopped.is_set():
            with self._state_lock:
                pending_batch = list(self._pending)
                self._pending = []
            if pending_batch:
                try:
                    self._flush(pending_batch)
                    backoff = self.RETRY_BACKOFF
                except Exception as e:
                    logger.error(
                        "Writer flush retry failed, next retry in %.1fs: %s", backoff, e
                    )
                    with self._state_lock:
                        self._pending = pending_batch + self._pending
                    self._stopped.wait(backoff)      # 停止时立即唤醒，不硬等
                    backoff = min(backoff * 2, self.MAX_RETRY_BACKOFF)
                continue
            try:
                item = self._buffer.get(timeout=0.1)
                batch.append(item)
            except queue.Empty:
                pass
            now = time.monotonic()
            if len(batch) >= self.BATCH_SIZE or (
                batch and now - self._last_flush >= self.FLUSH_INTERVAL
            ):
                try:
                    self._flush(batch)
                    self._last_flush = now
                    backoff = self.RETRY_BACKOFF
                except Exception as e:
                    logger.error("Writer flush failed, queued for retry: %s", e)
                    with self._state_lock:
                        self._pending = batch
                    backoff = self.RETRY_BACKOFF
                batch = []
        with self._state_lock:
            remaining = list(self._pending) + batch
            self._pending = []
        if remaining:
            try:
                self._flush(remaining)
            except Exception as e:
                logger.error("Final flush failed on stop: %s", e)
                final_failed = remaining
        with self._state_lock:
            self._final_failed_rows = final_failed

    # ------------------------------------------------------------------
    def _flush(self, batch: List[dict]) -> None:
        if not batch:
            return
        valid_items: List[dict] = []
        rows: List[dict] = []
        for item in batch:
            if not self._fields_valid(item):
                logger.error("Skipping invalid latency sample: %s/%s/%s",
                             item.get("channel"), item.get("metric"),
                             item.get("inst_id"))
                continue
            try:
                rows.append(self._to_row(item))
                valid_items.append(item)
            except ValueError as e:
                logger.error("Skipping latency sample (P0 guard): %s", e)
        if not valid_items:
            return
        # written 只计 market 行，按每行 sample_ts 窗口归属；UPSERT 只针对
        # 本批涉及的窗口（cumulative = 已提交累计 + 本批增量），避免长期运行
        # 时每批重发全部历史窗口。
        written_add = market_written_by_window(valid_items, self.summary_interval)
        # C-P0-5：_committed_written 读写用锁保护（stop() drain 可能与线程并发）
        with self._state_lock:
            stats_rows = [
                {
                    "window_start": w,
                    "source": self.source,
                    "metric": STAT_WRITTEN,
                    "value": self._committed_written.get(w, 0) + v,
                }
                for w, v in sorted(written_add.items())
            ]
            try:
                # 先更新注册表（消除窗口关闭快照竞态）；失败则回滚增量
                for w, v in written_add.items():
                    self.stats.add(w, {STAT_WRITTEN: v})
                with self.engine.begin() as conn:
                    conn.execute(pg_insert(LatencySample).values(rows))
                    if stats_rows:
                        conn.execute(_stats_upsert_stmt(stats_rows))
                for w, v in written_add.items():
                    self._committed_written[w] = self._committed_written.get(w, 0) + v
                self._prune_committed_written()
            except Exception:
                # 失败：回滚未提交的 written 增量，write_errors 按错误发生时间
                # 窗口 +1（attempt 计数）；重试时从 _committed_written 重新累计，
                # 不会重复计数
                for w, v in written_add.items():
                    self.stats.add(w, {STAT_WRITTEN: -v})
                self._count_write_error()
                raise

    def _prune_committed_written(self) -> None:
        """裁剪长期运行的 written 累计簿记（2 小时足够覆盖任何现实写入延迟）。"""
        horizon = datetime.now(timezone.utc) - timedelta(seconds=WRITTEN_PRUNE_HORIZON)
        for w in [w for w in self._committed_written if w < horizon]:
            self._committed_written.pop(w, None)

    def _count_write_error(self) -> None:
        w = window_start_for(datetime.now(timezone.utc), self.summary_interval)
        self.stats.increment(w, STAT_WRITE_ERRORS)
        self._upsert_stat(w, STAT_WRITE_ERRORS,
                          self.stats.window_value(w, STAT_WRITE_ERRORS))

    def _report_final_failed(self) -> None:
        if self._final_failed_reported:
            return
        self._final_failed_reported = True
        with self._state_lock:
            rows = list(self._final_failed_rows)
            self._final_failed_rows = []
        market = [r for r in rows if r.get("sample_class") == SAMPLE_CLASS_MARKET]
        if not market:
            return
        w = window_start_for(datetime.now(timezone.utc), self.summary_interval)
        self.stats.increment(w, STAT_FINAL_FAILED, len(market))
        self._upsert_stat(w, STAT_FINAL_FAILED,
                          self.stats.window_value(w, STAT_FINAL_FAILED))
        logger.error(
            "final_write_failed_samples=%d (rows still unwritten at shutdown; "
            "reasons: DB flush failed)", len(market),
        )

    def _upsert_stat(self, window, metric: str, value: int) -> None:
        try:
            rows = [
                {"window_start": window, "source": self.source, "metric": metric,
                 "value": int(value)}
            ]
            upsert_stats(self.engine, rows)
        except Exception as e:
            logger.warning("Stats UPSERT failed (%s=%s): %s", metric, value, e)

    # ------------------------------------------------------------------
    @staticmethod
    def _fields_valid(item: dict) -> bool:
        if item.get("metric") == CORRECTED_METRIC:
            return False
        if len(str(item.get("source", ""))) > MAX_SOURCE_LEN:
            return False
        if len(str(item.get("channel", ""))) > MAX_CHANNEL_LEN:
            return False
        if len(str(item.get("metric", ""))) > MAX_METRIC_LEN:
            return False
        if len(str(item.get("inst_id", ""))) > MAX_INST_ID_LEN:
            return False
        return True

    @staticmethod
    def _to_row(item: dict) -> dict:
        metric = item["metric"]
        if metric == CORRECTED_METRIC:
            raise ValueError("corrected_ws_receive_latency must never be persisted")
        return {
            "sample_ts": item["sample_ts"],
            "session": item["session"],
            "source": item["source"],
            "inst_id": item["inst_id"],
            "channel": item["channel"],
            "metric": metric,
            "value_ms": float(item["value_ms"]),
            "exchange_ts": item.get("exchange_ts"),
            "recv_ts": item["recv_ts"],
            "clock_offset_ms": item.get("clock_offset_ms"),
        }
