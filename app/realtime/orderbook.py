"""OrderBook 实时处理器

实现本地 OrderBookState 重建、seq gap 检测、resync、快照持久化。
"""

import threading
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime

logger = get_logger(__name__)


class OrderBookState:
    """OrderBook 内存状态对象"""

    def __init__(self, inst_id: str, depth: int = 400):
        self.inst_id = inst_id
        self.depth = depth
        self.bids: List[List[str]] = []  # [[price, size], ...] 按价格降序
        self.asks: List[List[str]] = []  # [[price, size], ...] 按价格升序
        self.seq_id: Optional[int] = None
        self.prev_seq_id: Optional[int] = None
        self.last_update_ts: Optional[datetime] = None
        self.state = "INIT"
        self.resync_count = 0
        self.last_resync_reason: Optional[str] = None
        self._lock = threading.Lock()

    def apply_snapshot(self, data: dict) -> None:
        """应用 snapshot 数据"""
        with self._lock:
            self.bids = self._sort_bids(data.get("bids", []))
            self.asks = self._sort_asks(data.get("asks", []))
            self.seq_id = self._to_int(data.get("seqId"))
            self.prev_seq_id = self._to_int(data.get("prevSeqId"))
            self.last_update_ts = self._parse_ts(data.get("ts"))
            self.state = "RUNNING"
            self.last_resync_reason = None

    def apply_update(self, data: dict) -> bool:
        """应用 update 数据

        Returns:
            bool: True if applied, False if gap detected
        """
        with self._lock:
            if self.state != "RUNNING":
                return False

            new_seq = self._to_int(data.get("seqId"))
            new_prev = self._to_int(data.get("prevSeqId"))

            if new_prev == self.seq_id:
                self._apply_delta(data.get("bids", []), data.get("asks", []))
                self.seq_id = new_seq
                self.prev_seq_id = new_prev
                self.last_update_ts = self._parse_ts(data.get("ts"))
                return True

            # 官方序列重置（维护导致 seqId 归零）
            if self._is_sequence_reset(new_seq, new_prev):
                logger.warning(
                    "OrderBook SEQUENCE_RESET: %s | old_seq=%s | new_seq=%s | new_prev=%s",
                    self.inst_id, self.seq_id, new_seq, new_prev
                )
                self.seq_id = new_seq
                self.prev_seq_id = new_prev
                self._apply_delta(data.get("bids", []), data.get("asks", []))
                self.last_update_ts = self._parse_ts(data.get("ts"))
                return True

            logger.warning(
                "OrderBook seq gap detected: %s | local_seq=%s | new_prev=%s | new_seq=%s",
                self.inst_id, self.seq_id, new_prev, new_seq
            )
            self.state = "GAP_DETECTED"
            self.last_resync_reason = "SEQ_GAP"
            return False

    def prepare_resync(self) -> None:
        """进入 RESYNCING 状态，清空本地状态"""
        with self._lock:
            self.bids = []
            self.asks = []
            self.seq_id = None
            self.prev_seq_id = None
            self.state = "RESYNCING"
            self.resync_count += 1

    def snapshot(self) -> Tuple[List[List[str]], List[List[str]]]:
        """获取当前盘口快照副本"""
        with self._lock:
            return deepcopy(self.bids[:self.depth]), deepcopy(self.asks[:self.depth])

    def best_bid(self) -> Optional[List[str]]:
        with self._lock:
            return self.bids[0] if self.bids else None

    def best_ask(self) -> Optional[List[str]]:
        with self._lock:
            return self.asks[0] if self.asks else None

    def _apply_delta(self, bid_updates: List, ask_updates: List) -> None:
        self.bids = self._update_side(self.bids, bid_updates, descending=True)
        self.asks = self._update_side(self.asks, ask_updates, descending=False)

    def _update_side(
        self, side: List[List[str]], updates: List[List[str]], descending: bool
    ) -> List[List[str]]:
        book = {row[0]: row[1] for row in side}
        for u in updates:
            price = u[0]
            size = u[1]
            if size == "0" or size == "0.0" or size == "":
                book.pop(price, None)
            else:
                book[price] = size
        result = [[p, s] for p, s in book.items()]
        result.sort(key=lambda x: Decimal(x[0]), reverse=descending)
        return result

    @staticmethod
    def _sort_bids(bids: List[List[str]]) -> List[List[str]]:
        return sorted(
            [[b[0], b[1]] for b in bids],
            key=lambda x: Decimal(x[0]),
            reverse=True,
        )

    @staticmethod
    def _sort_asks(asks: List[List[str]]) -> List[List[str]]:
        return sorted(
            [[a[0], a[1]] for a in asks],
            key=lambda x: Decimal(x[0]),
        )

    @staticmethod
    def _to_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_ts(value) -> Optional[datetime]:
        if value is None:
            return None
        try:
            return ms_to_datetime(int(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _is_sequence_reset(new_seq: Optional[int], new_prev: Optional[int]) -> bool:
        """判断是否为官方序列重置（seqId 显著变小）"""
        if new_seq is None or new_prev is None:
            return False
        return new_seq < new_prev and new_seq < 1000


class OrderBookHandler:
    """处理 OKX orderbook 频道 WS 消息"""

    def __init__(self, inst_id: str, channel: str = "books", depth: int = 400):
        self.inst_id = inst_id
        self.channel = channel
        self.depth = depth
        self.state = OrderBookState(inst_id, depth=depth)

    def handle(self, data: dict) -> Optional[dict]:
        """处理 WS 消息

        Returns:
            dict or None: 处理后的快照信息（仅 PERIODIC/RESYNC 快照）
        """
        arg = data.get("arg", {})
        if arg.get("channel") not in ("books", "books5", "books50-l2-tbt", "books-l2-tbt", "bbo-tbt"):
            return None

        action = data.get("action")
        raw_data = data.get("data", [])
        if not raw_data:
            return None

        item = raw_data[0]
        # OKX books 快照/更新 data 项不一定包含 instId，从 arg 兜底
        item_inst_id = item.get("instId") or arg.get("instId")
        if item_inst_id != self.inst_id:
            return None

        if action == "snapshot":
            if self.state.state != "INIT":
                self.state.prepare_resync()
            self.state.apply_snapshot(item)
            logger.info(
                "OrderBook snapshot ready: %s | seq=%s | bids=%d | asks=%d",
                self.inst_id, self.state.seq_id, len(self.state.bids), len(self.state.asks)
            )
            return self._build_snapshot_record(item, snapshot_type="RESYNC" if self.state.resync_count > 0 else "INITIAL")

        if action == "update":
            if self.state.state == "RESYNCING":
                # RESYNCING 期间丢弃所有 update
                return None
            ok = self.state.apply_update(item)
            if not ok:
                self.state.prepare_resync()
                return None
            return None

        return None

    def periodic_snapshot(self) -> Optional[dict]:
        """生成周期性快照记录"""
        if self.state.state != "RUNNING":
            return None
        bids, asks = self.state.snapshot()
        if not bids or not asks:
            return None
        now = datetime.now(timezone.utc)
        best_bid = bids[0]
        best_ask = asks[0]
        return {
            "inst_id": self.inst_id,
            "ts": self.state.last_update_ts or now,
            "exchange_ts": self.state.last_update_ts or now,
            "snapshot_at": now,
            "bids": bids[:5],
            "asks": asks[:5],
            "best_bid_px": best_bid[0],
            "best_bid_sz": best_bid[1],
            "best_ask_px": best_ask[0],
            "best_ask_sz": best_ask[1],
            "seq_id": self.state.seq_id,
            "prev_seq_id": self.state.prev_seq_id,
            "source": "WS",
            "snapshot_type": "PERIODIC",
        }

    def _build_snapshot_record(self, item: dict, snapshot_type: str = "INITIAL") -> dict:
        now = datetime.now(timezone.utc)
        bids = self.state.bids[:5]
        asks = self.state.asks[:5]
        best_bid = bids[0] if bids else [None, None]
        best_ask = asks[0] if asks else [None, None]
        return {
            "inst_id": self.inst_id,
            "ts": self.state.last_update_ts or now,
            "exchange_ts": self.state.last_update_ts or now,
            "snapshot_at": now,
            "bids": bids,
            "asks": asks,
            "best_bid_px": best_bid[0],
            "best_bid_sz": best_bid[1],
            "best_ask_px": best_ask[0],
            "best_ask_sz": best_ask[1],
            "seq_id": self.state.seq_id,
            "prev_seq_id": self.state.prev_seq_id,
            "checksum": item.get("checksum"),
            "source": "WS",
            "snapshot_type": snapshot_type,
        }
