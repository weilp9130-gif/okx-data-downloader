"""Trades 实时处理器"""

from datetime import datetime, timezone
from typing import Optional

from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime
from .writer import TradeWriter

logger = get_logger(__name__)


class TradesRealtimeHandler:
    """处理 OKX trades 频道 WS 消息"""

    def __init__(self, writer: TradeWriter):
        self.writer = writer
        self._count = 0

    def handle(self, data: dict) -> int:
        """处理单条 WS 消息

        Args:
            data: OKX WS 消息 dict

        Returns:
            int: 处理的 trade 数量
        """
        if data.get("arg", {}).get("channel") != "trades":
            return 0
        trades = data.get("data", [])
        received_at = datetime.now(timezone.utc)
        count = 0
        for raw in trades:
            inst_id = raw.get("instId")
            if not inst_id:
                continue
            try:
                record = self._normalize(raw, received_at)
            except Exception as e:
                logger.warning("Skip invalid trade: %s | %s", raw, e)
                continue
            self.writer.put(record)
            count += 1
        self._count += count
        return count

    def _normalize(self, raw: dict, received_at: datetime) -> dict:
        return {
            "inst_id": raw["instId"],
            "trade_id": raw["tradeId"],
            "ts": ms_to_datetime(int(raw["ts"])),
            "px": raw["px"],
            "sz": raw["sz"],
            "side": raw["side"],
            "source": "WS",
            "received_at": received_at,
            "raw_json": raw,
        }

    @property
    def count(self) -> int:
        return self._count
