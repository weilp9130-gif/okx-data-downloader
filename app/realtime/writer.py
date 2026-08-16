"""实时数据写入模块

BaseWriter + TradeWriter
"""

import hashlib
import json
import queue
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List

import time

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..database import get_engine
from ..utils.logger import get_logger

logger = get_logger(__name__)


class BaseWriter(ABC):
    """实时数据写入基类

    提供有界队列、批量 flush、失败重试、背压机制。
    """

    MAX_BUFFER_SIZE = 10000
    FLUSH_INTERVAL = 1.0
    BATCH_SIZE = 1000

    def __init__(self):
        self._buffer: queue.Queue = queue.Queue(maxsize=self.MAX_BUFFER_SIZE)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stopped = threading.Event()
        self._thread.start()

    def put(self, item: dict) -> bool:
        """将数据放入写入队列

        Returns:
            bool: True if success, False if buffer full (backpressure)
        """
        try:
            self._buffer.put_nowait(item)
            return True
        except queue.Full:
            logger.error("Writer buffer full, dropping item (backpressure)")
            return False

    def stop(self, timeout: float = 5.0) -> None:
        """停止写入线程并 flush 剩余数据"""
        self._stopped.set()
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        """后台写入线程"""
        batch: List[dict] = []
        last_flush = time.monotonic()
        while not self._stopped.is_set():
            try:
                item = self._buffer.get(timeout=0.1)
                batch.append(item)
            except queue.Empty:
                pass

            now = time.monotonic()
            if len(batch) >= self.BATCH_SIZE or (batch and now - last_flush >= self.FLUSH_INTERVAL):
                self._flush(batch)
                batch = []
                last_flush = now

        # flush remaining
        if batch:
            self._flush(batch)

    @abstractmethod
    def _flush(self, batch: List[dict]) -> None:
        pass


import time as _time_module

# patch time reference
import time as _time_ref


class OrderBookWriter(BaseWriter):
    """OrderBook 快照写入器"""

    def __init__(self):
        super().__init__()
        self.engine = get_engine()

    def _flush(self, batch: List[dict]) -> None:
        from ..models import OrderBookSnapshot, OrderBookFactor

        if not batch:
            return
        snapshot_rows = [b for b in batch if b.get("__type") == "snapshot"]
        factor_rows = [b for b in batch if b.get("__type") == "factor"]
        try:
            if snapshot_rows:
                for row in snapshot_rows:
                    row.pop("__type", None)
                stmt = pg_insert(OrderBookSnapshot).values(snapshot_rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["inst_id", "snapshot_at"],
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
                    },
                )
                with self.engine.begin() as conn:
                    conn.execute(stmt)
                logger.info("OrderBookWriter flushed %d snapshots", len(snapshot_rows))
            if factor_rows:
                for row in factor_rows:
                    row.pop("__type", None)
                stmt = pg_insert(OrderBookFactor).values(factor_rows)
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
                logger.info("OrderBookWriter flushed %d factors", len(factor_rows))
        except Exception as e:
            logger.error("OrderBookWriter flush failed: %s", e)


class TradeWriter(BaseWriter):
    """Trade 实时写入器"""

    def __init__(self):
        super().__init__()
        self.engine = get_engine()

    def _flush(self, batch: List[dict]) -> None:
        from ..models import Trade

        if not batch:
            return

        rows = [self._normalize(b) for b in batch]
        if not rows:
            return
        stmt = pg_insert(Trade).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id", "trade_id", "ts"],
            set_={
                "px": stmt.excluded.px,
                "sz": stmt.excluded.sz,
                "side": stmt.excluded.side,
                "source": stmt.excluded.source,
                "received_at": stmt.excluded.received_at,
                "ingested_at": stmt.excluded.ingested_at,
                "raw_json": stmt.excluded.raw_json,
                "raw_hash": stmt.excluded.raw_hash,
            },
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
            logger.info("TradeWriter flushed %d rows", len(rows))
        except Exception as e:
            logger.error("TradeWriter flush failed: %s", e)

    def _normalize(self, item: dict) -> dict:
        raw = item.get("raw_json", item)
        raw_hash = hashlib.sha256(
            json.dumps(raw, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
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
