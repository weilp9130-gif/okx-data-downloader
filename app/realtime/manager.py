"""WebSocket 实时管理器"""

import asyncio
import threading
from typing import Dict, List, Optional

from ..utils.logger import get_logger
from .market_data import MarketDataHandler
from .okx_ws import OKXWebSocketClient
from .orderbook import OrderBookHandler
from .recovery import TradeRecovery
from .trades import TradesRealtimeHandler
from .writer import MarketDataWriter, OrderBookWriter, TradeWriter

logger = get_logger(__name__)

# 市场数据频道 -> OKX channel 前缀（orderbook 与 trades 单独处理）
MARKET_DATA_CHANNELS = {"oi", "funding", "mark", "index", "kline"}

# 市场数据频道对应的订阅参数构造
MARKET_DATA_SUBSCRIBE = {
    "oi": {"channel": "open-interest"},
    "funding": {"channel": "funding-rate"},
    "mark": {"channel": "mark-price"},
    "index": {"channel": "index-tickers"},
    "kline": {"channel": "candle1m"},
}


class RealtimeManager:
    """WebSocket 实时采集管理器

    支持频道：
        trades      -> trades
        orderbook   -> books
        oi          -> open-interest
        funding     -> funding-rate
        mark        -> mark-price
        index       -> index-tickers
        kline       -> candle{bar}
    """

    def __init__(self, inst_ids: List[str], channels: List[str] = None):
        self.inst_ids = inst_ids
        self.channels = channels or ["trades"]
        self.use_trades = "trades" in self.channels
        self.use_orderbook = "orderbook" in self.channels or "books" in self.channels
        self.use_market_data = any(c in self.channels for c in MARKET_DATA_CHANNELS)
        self.trade_writer: Optional[TradeWriter] = None
        self.trade_handler: Optional[TradesRealtimeHandler] = None
        self.orderbook_writer: Optional[OrderBookWriter] = None
        self.orderbook_handlers: Dict[str, OrderBookHandler] = {}
        self.market_data_writer: Optional[MarketDataWriter] = None
        self.market_data_handler = MarketDataHandler()
        self.recovery = TradeRecovery()
        self.client: Optional[OKXWebSocketClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        if self.use_trades:
            self.trade_writer = TradeWriter()
            self.trade_handler = TradesRealtimeHandler(self.trade_writer)
        if self.use_orderbook:
            self.orderbook_writer = OrderBookWriter()
            for inst_id in self.inst_ids:
                self.orderbook_handlers[inst_id] = OrderBookHandler(inst_id)
        if self.use_market_data:
            self.market_data_writer = MarketDataWriter()

    def start(self) -> None:
        """在新线程中启动 WebSocket 连接"""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """停止采集"""
        if self.client:
            asyncio.run_coroutine_threadsafe(self.client.close(), self._loop)
        if self.trade_writer:
            self.trade_writer.stop(timeout=timeout)
        if self.orderbook_writer:
            self.orderbook_writer.stop(timeout=timeout)
        if self.market_data_writer:
            self.market_data_writer.stop(timeout=timeout)
        if self._thread:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        def on_message(data: dict):
            if self.trade_handler:
                self.trade_handler.handle(data)
            if self.orderbook_writer:
                self._handle_orderbook(data)
            if self.market_data_writer:
                self._handle_market_data(data)

        self.client = OKXWebSocketClient(on_message=on_message)
        # 启动时执行一次 trades 恢复
        if self.use_trades:
            for inst_id in self.inst_ids:
                try:
                    self.recovery.recover(inst_id)
                except Exception as e:
                    logger.error("Recovery failed for %s: %s", inst_id, e)

        self._loop.run_until_complete(self._connect_and_run())

    def _handle_orderbook(self, data: dict) -> None:
        arg = data.get("arg", {})
        inst_id = arg.get("instId")
        if not inst_id or inst_id not in self.orderbook_handlers:
            return
        handler = self.orderbook_handlers[inst_id]
        record = handler.handle(data)
        if record and self.orderbook_writer:
            record["__type"] = "snapshot"
            self.orderbook_writer.put(record)

    def _handle_market_data(self, data: dict) -> None:
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        if channel.startswith("candle"):
            inst_id = arg.get("instId")
            if not inst_id:
                return
            from datetime import datetime, timezone
            bar = channel[len("candle"):]
            records = self.market_data_handler.handle_candles_with_inst(
                data, bar, inst_id, datetime.now(timezone.utc)
            )
        else:
            records = self.market_data_handler.handle(data)
        for item in records:
            item["__target"] = item["target"]
            self.market_data_writer.put(item)

    async def _connect_and_run(self) -> None:
        args = []
        if self.use_trades:
            args.extend([{"channel": "trades", "instId": i} for i in self.inst_ids])
        if self.use_orderbook:
            args.extend([{"channel": "books", "instId": i} for i in self.inst_ids])
        for ch in self.channels:
            if ch in MARKET_DATA_SUBSCRIBE:
                template = MARKET_DATA_SUBSCRIBE[ch]
                args.extend(
                    [{"channel": template["channel"], "instId": i} for i in self.inst_ids]
                )
        self.client.set_subscriptions(args)
        await self.client.connect()
