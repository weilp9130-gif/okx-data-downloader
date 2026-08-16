"""WebSocket 实时管理器（Phase 8 统一调度）

分层：
    SubscriptionManager  -> 订阅参数构建与频道映射
    Data Handlers        -> trades / orderbook / market_data
    RealtimeManager      -> 生命周期、消息分发、断线恢复
"""

import asyncio
import threading
from typing import Dict, List, Optional

from ..utils.logger import get_logger
from .market_data import MarketDataHandler
from .okx_ws import OKXWebSocketClient
from .orderbook import OrderBookHandler
from .recovery import RecoveryManager
from .trades import TradesRealtimeHandler
from .writer import MarketDataWriter, OrderBookWriter, TradeWriter

logger = get_logger(__name__)

# 市场数据频道 -> OKX channel 前缀
MARKET_DATA_CHANNELS = {"oi", "funding", "mark", "index", "kline"}

# 市场数据频道对应的订阅参数构造
MARKET_DATA_SUBSCRIBE = {
    "oi": {"channel": "open-interest"},
    "funding": {"channel": "funding-rate"},
    "mark": {"channel": "mark-price"},
    "index": {"channel": "index-tickers"},
    "kline": {"channel": "candle1m"},
}

# 频道 -> 断线恢复数据类型
CHANNEL_RECOVERY_TYPE = {
    "trades": "trades",
    "oi": "oi",
    "mark": "mark",
    "index": "index",
    "funding": "funding",
    "orderbook": "orderbook",
}


class SubscriptionManager:
    """订阅参数管理：channel + inst_id 映射"""

    def __init__(self, inst_ids: List[str], channels: List[str]):
        self.inst_ids = inst_ids
        self.channels = list(channels)

    @property
    def args(self) -> List[dict]:
        """生成订阅参数列表"""
        args: List[dict] = []
        if "trades" in self.channels:
            args.extend([{"channel": "trades", "instId": i} for i in self.inst_ids])
        if "orderbook" in self.channels or "books" in self.channels:
            args.extend([{"channel": "books", "instId": i} for i in self.inst_ids])
        for ch in self.channels:
            if ch in MARKET_DATA_SUBSCRIBE:
                template = MARKET_DATA_SUBSCRIBE[ch]
                args.extend(
                    [{"channel": template["channel"], "instId": i} for i in self.inst_ids]
                )
        return args


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
        self.subscriptions = SubscriptionManager(inst_ids, self.channels)
        self.use_trades = "trades" in self.channels
        self.use_orderbook = "orderbook" in self.channels or "books" in self.channels
        self.use_market_data = any(c in self.channels for c in MARKET_DATA_CHANNELS)
        self.trade_writer: Optional[TradeWriter] = None
        self.trade_handler: Optional[TradesRealtimeHandler] = None
        self.orderbook_writer: Optional[OrderBookWriter] = None
        self.orderbook_handlers: Dict[str, OrderBookHandler] = {}
        self.market_data_writer: Optional[MarketDataWriter] = None
        self.market_data_handler = MarketDataHandler()
        self.recovery = RecoveryManager()
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

        def on_reconnect():
            self._run_recovery(reason="WS_RECONNECT")

        self.client = OKXWebSocketClient(
            on_message=on_message,
            on_reconnect=on_reconnect,
        )
        # 启动时执行一次恢复
        self._run_recovery(reason="STARTUP")

        self._loop.run_until_complete(self._connect_and_run())

    def _run_recovery(self, reason: str = "WS_RECONNECT") -> None:
        """对启用频道执行断线恢复

        OrderBook 不在此处恢复：其 resync 由 OrderBookHandler 状态机在
        检测到 seq gap 时触发，等待 action=snapshot 后关闭缺口。
        """
        recovery_types = set()
        for ch in self.channels:
            dt = CHANNEL_RECOVERY_TYPE.get(ch)
            if dt and dt != "orderbook":
                recovery_types.add(dt)
        if not recovery_types:
            return
        for inst_id in self.inst_ids:
            for dt in sorted(recovery_types):
                try:
                    self.recovery.recover(dt, inst_id, reason=reason)
                except Exception as e:
                    logger.error("Recovery failed: %s %s | %s", dt, inst_id, e)

    def _handle_orderbook(self, data: dict) -> None:
        arg = data.get("arg", {})
        inst_id = arg.get("instId")
        if not inst_id or inst_id not in self.orderbook_handlers:
            return
        handler = self.orderbook_handlers[inst_id]
        prev_state = handler.state.state
        record = handler.handle(data)
        # seq gap 后进入 RESYNCING：登记缺口
        if handler.state.state == "RESYNCING" and prev_state != "RESYNCING":
            try:
                self.recovery.orderbook.trigger_resync(
                    inst_id, reason=handler.state.last_resync_reason or "SEQ_GAP"
                )
            except Exception as e:
                logger.error("OrderBook gap registration failed: %s | %s", inst_id, e)
        if record and self.orderbook_writer:
            record["__type"] = "snapshot"
            self.orderbook_writer.put(record)
            # 新 snapshot 到达且此前有 resync：关闭 OPEN 缺口
            if record.get("snapshot_type") == "RESYNC":
                try:
                    self.recovery.orderbook.mark_snapshot_ready(inst_id)
                except Exception as e:
                    logger.error("OrderBook gap close failed: %s | %s", inst_id, e)

    def _handle_market_data(self, data: dict) -> None:
        records = self.market_data_handler.handle(data)
        for item in records:
            item["__target"] = item["target"]
            self.market_data_writer.put(item)

    async def _connect_and_run(self) -> None:
        self.client.set_subscriptions(self.subscriptions.args)
        await self.client.connect()
