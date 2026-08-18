"""实时数据写入模块

BaseWriter + TradeWriter + OrderBookWriter + MarketDataWriter
"""

import queue
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..quality.conflict import DataConflictDetector, trade_core_hash
from ..db.database import get_engine
from ..utils.logger import get_logger

logger = get_logger(__name__)


class BaseWriter(ABC):
    """实时数据写入基类

    提供有界队列、批量 flush、失败重试、背压机制。
    - 缓冲满：put 阻塞最多 PUT_TIMEOUT 秒（生产侧放慢而非直接丢数据）
    - batch 失败：回退指数退避后重试（不静默丢失）
    """

    MAX_BUFFER_SIZE = 10000
    FLUSH_INTERVAL = 1.0
    BATCH_SIZE = 1000
    PUT_TIMEOUT = 1.0
    RETRY_BACKOFF = 2.0
    MAX_RETRY_BACKOFF = 30.0

    def __init__(self):
        self._buffer: queue.Queue = queue.Queue(maxsize=self.MAX_BUFFER_SIZE)
        self._pending: List[dict] = []  # 失败待重试批次（保序）
        self._stopped = threading.Event()
        self._last_flush = time.monotonic()
        self._drops = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def put(self, item: dict) -> bool:
        """将数据放入写入队列

        Returns:
            bool: True if success, False if buffer still full after PUT_TIMEOUT
        """
        try:
            self._buffer.put(item, timeout=self.PUT_TIMEOUT)
            return True
        except queue.Full:
            self._drops += 1
            logger.error(
                "Writer buffer full after %.1fs, dropped %d items (backpressure)",
                self.PUT_TIMEOUT, self._drops,
            )
            return False

    @property
    def drops(self) -> int:
        return self._drops

    def stop(self, timeout: float = 5.0) -> None:
        """停止写入线程并尝试 flush 剩余数据"""
        self._stopped.set()
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        """后台写入线程：优先重试队列，其次消费缓冲"""
        batch: List[dict] = []
        backoff = self.RETRY_BACKOFF
        while not self._stopped.is_set():
            # 1) 先处理失败重试队列
            if self._pending:
                pending_batch = self._pending
                self._pending = []
                try:
                    self._flush(pending_batch)
                    backoff = self.RETRY_BACKOFF
                except Exception as e:
                    logger.error(
                        "Writer flush retry failed, next retry in %.1fs: %s", backoff, e
                    )
                    self._pending = pending_batch + self._pending
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.MAX_RETRY_BACKOFF)
                continue

            # 2) 消费缓冲
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
                    self._pending = batch
                    backoff = self.RETRY_BACKOFF
                batch = []

        # 3) 停止前尝试 flush 剩余（失败则记录，进程退出前已尽力）
        remaining = self._pending + batch
        if remaining:
            try:
                self._flush(remaining)
            except Exception as e:
                logger.error("Final flush failed on stop: %s", e)

    @abstractmethod
    def _flush(self, batch: List[dict]) -> None:
        pass


class OrderBookWriter(BaseWriter):
    """OrderBook 快照 / 因子 / 同步状态写入器"""

    def __init__(self):
        super().__init__()
        self.engine = get_engine()

    def _flush(self, batch: List[dict]) -> None:
        if not batch:
            return
        snapshot_rows = [b for b in batch if b.get("__type") == "snapshot"]
        factor_rows = [b for b in batch if b.get("__type") == "factor"]
        state_rows = [b for b in batch if b.get("__type") == "sync_state"]

        if snapshot_rows:
            self._flush_snapshots(snapshot_rows)
        if factor_rows:
            self._flush_factors(factor_rows)
        if state_rows:
            self._flush_sync_state(state_rows)

    def _flush_snapshots(self, rows: List[dict]) -> None:
        from ..db.models import OrderBookSnapshot

        for row in rows:
            row.pop("__type", None)
        stmt = pg_insert(OrderBookSnapshot).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "snapshot_at", "ts"],
            set_={
                "bids": stmt.excluded.bids,
                "asks": stmt.excluded.asks,
                "best_bid_px": stmt.excluded.best_bid_px,
                "best_bid_sz": stmt.excluded.best_bid_sz,
                "best_ask_px": stmt.excluded.best_ask_px,
                "best_ask_sz": stmt.excluded.best_ask_sz,
                "seq_id": stmt.excluded.seq_id,
                "prev_seq_id": stmt.excluded.prev_seq_id,
                "checksum": stmt.excluded.checksum,
                "source": stmt.excluded.source,
                "snapshot_type": stmt.excluded.snapshot_type,
                "received_at": stmt.excluded.received_at,
            },
        )
        # 失败由 BaseWriter 重试，不再吞异常
        with self.engine.begin() as conn:
            conn.execute(stmt)
        logger.info("OrderBookWriter flushed %d snapshots", len(rows))

    def _flush_factors(self, rows: List[dict]) -> None:
        from ..db.models import OrderBookFactor

        for row in rows:
            row.pop("__type", None)
        stmt = pg_insert(OrderBookFactor).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "ts"],
            set_={
                "spread": stmt.excluded.spread,
                "mid": stmt.excluded.mid,
                "wmid": stmt.excluded.wmid,
                "bid_depth_5": stmt.excluded.bid_depth_5,
                "ask_depth_5": stmt.excluded.ask_depth_5,
                "bid_depth_10": stmt.excluded.bid_depth_10,
                "ask_depth_10": stmt.excluded.ask_depth_10,
                "imbalance_5": stmt.excluded.imbalance_5,
                "imbalance_10": stmt.excluded.imbalance_10,
            },
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)
        logger.info("OrderBookWriter flushed %d factors", len(rows))

    def _flush_sync_state(self, rows: List[dict]) -> None:
        from ..db.models import OrderBookSyncState

        # 同一 inst_id 只保留最新一条，避免同批 UPSERT 冲突
        latest: dict = {}
        for row in rows:
            row.pop("__type", None)
            latest[row["inst_id"]] = row
        values = list(latest.values())
        stmt = pg_insert(OrderBookSyncState).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id"],
            set_={
                "prev_seq": stmt.excluded.prev_seq,
                "latest_seq": stmt.excluded.latest_seq,
                "latest_ts": stmt.excluded.latest_ts,
                "resync_count": stmt.excluded.resync_count,
                "last_resync_reason": stmt.excluded.last_resync_reason,
                "status": stmt.excluded.status,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)


class MarketDataWriter(BaseWriter):
    """WebSocket 市场数据写入器（OI / Funding / Mark / Index / Kline）

    接收 MarketDataHandler 产出的 {target, record} 项，按目标表分别写入。
    """

    def __init__(self):
        super().__init__()
        self.engine = get_engine()

    def _flush(self, batch: List[dict]) -> None:
        if not batch:
            return
        grouped: dict = {}
        for item in batch:
            target = item.get("__target")
            record = item.get("record")
            if not target or not record:
                continue
            grouped.setdefault(target, []).append(record)

        for target, rows in grouped.items():
            # 失败时抛给 BaseWriter 重试整个 batch，避免半写入
            self._flush_target(target, rows)

    def _flush_target(self, target: str, rows: List[dict]) -> None:
        if target == "open_interest_realtime":
            from ..db.models import OpenInterestRealtime
            stmt = pg_insert(OpenInterestRealtime).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["inst_id", "ts"],
                set_={
                    "oi": stmt.excluded.oi,
                    "oi_ccy": stmt.excluded.oi_ccy,
                    "oi_usd": stmt.excluded.oi_usd,
                    "raw_json": stmt.excluded.raw_json,
                    "received_at": stmt.excluded.received_at,
                    "ingested_at": stmt.excluded.ingested_at,
                },
            )
        elif target == "funding_rates":
            from ..db.models import FundingRate
            stmt = pg_insert(FundingRate).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["inst_id", "ts"],
                set_={
                    "funding_rate": stmt.excluded.funding_rate,
                    "realized_rate": stmt.excluded.realized_rate,
                    "funding_time": stmt.excluded.funding_time,
                },
            )
        elif target == "mark_prices":
            from ..db.models import MarkPrice
            stmt = pg_insert(MarkPrice).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["inst_id", "bar", "ts"],
                set_={
                    "o": stmt.excluded.o,
                    "h": stmt.excluded.h,
                    "l": stmt.excluded.l,
                    "c": stmt.excluded.c,
                    "source": stmt.excluded.source,
                    "received_at": stmt.excluded.received_at,
                    "fetched_at": stmt.excluded.fetched_at,
                    "raw_json": stmt.excluded.raw_json,
                    "ingested_at": stmt.excluded.ingested_at,
                },
            )
        elif target == "index_prices":
            from ..db.models import IndexPrice
            stmt = pg_insert(IndexPrice).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["inst_id", "bar", "ts"],
                set_={
                    "o": stmt.excluded.o,
                    "h": stmt.excluded.h,
                    "l": stmt.excluded.l,
                    "c": stmt.excluded.c,
                    "source": stmt.excluded.source,
                    "received_at": stmt.excluded.received_at,
                    "fetched_at": stmt.excluded.fetched_at,
                    "raw_json": stmt.excluded.raw_json,
                    "ingested_at": stmt.excluded.ingested_at,
                },
            )
        elif target == "candles":
            from ..db.models import Candle
            stmt = pg_insert(Candle).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["inst_id", "bar", "ts"],
                set_={
                    "o": stmt.excluded.o,
                    "h": stmt.excluded.h,
                    "l": stmt.excluded.l,
                    "c": stmt.excluded.c,
                    "vol": stmt.excluded.vol,
                },
            )
        else:
            logger.warning("MarketDataWriter unknown target: %s", target)
            return

        with self.engine.begin() as conn:
            conn.execute(stmt)
        logger.info("MarketDataWriter flushed %d rows -> %s", len(rows), target)


class TradeWriter(BaseWriter):
    """Trade 实时写入器"""

    def __init__(self):
        super().__init__()
        self.engine = get_engine()
        self.conflicts = DataConflictDetector()

    def _flush(self, batch: List[dict]) -> None:
        from ..db.models import Trade

        if not batch:
            return

        rows = [self._normalize(b) for b in batch]
        if not rows:
            return

        # Raw 数据不可静默覆盖：同键不同 payload 登记 DATA_CONFLICT
        try:
            rows, conflicts = self.conflicts.detect_trades(rows)
            if conflicts:
                logger.error(
                    "TradeWriter 检测到 %d 条 DATA_CONFLICT，已登记并跳过", len(conflicts)
                )
        except Exception as e:
            logger.error("TradeWriter conflict detection failed: %s", e)

        if not rows:
            return

        stmt = pg_insert(Trade).values(rows)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["inst_id", "trade_id", "ts"]
        )
        # 失败抛给 BaseWriter 重试
        with self.engine.begin() as conn:
            conn.execute(stmt)
        logger.info("TradeWriter flushed %d rows", len(rows))

    def _normalize(self, item: dict) -> dict:
        raw = item.get("raw_json", item)
        raw_hash = trade_core_hash(raw)
        return {
            "inst_id": item["inst_id"],
            "trade_id": item["trade_id"],
            "ts": item["ts"],
            "px": item["px"],
            "sz": item["sz"],
            "side": item["side"],
            "source": "WS",
            "received_at": item.get("received_at"),
            "ingested_at": datetime.now(timezone.utc),
            "fill_time": item["ts"],
            "raw_json": raw,
            "raw_hash": raw_hash,
        }
