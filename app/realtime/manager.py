"""WebSocket 实时管理器"""

import asyncio
import threading
from typing import List, Optional

from ..utils.logger import get_logger
from .okx_ws import OKXWebSocketClient
from .recovery import TradeRecovery
from .trades import TradesRealtimeHandler
from .writer import TradeWriter

logger = get_logger(__name__)


class RealtimeManager:
    """WebSocket 实时采集管理器

    Phase 4 仅支持 trades 频道。
    """

    def __init__(self, inst_ids: List[str]):
        self.inst_ids = inst_ids
        self.writer = TradeWriter()
        self.trade_handler = TradesRealtimeHandler(self.writer)
        self.recovery = TradeRecovery()
        self.client: Optional[OKXWebSocketClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """在新线程中启动 WebSocket 连接"""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """停止采集"""
        if self.client:
            asyncio.run_coroutine_threadsafe(self.client.close(), self._loop)
        self.writer.stop(timeout=timeout)
        if self._thread:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        def on_message(data: dict):
            self.trade_handler.handle(data)

        self.client = OKXWebSocketClient(on_message=on_message)
        # 启动时执行一次恢复
        for inst_id in self.inst_ids:
            try:
                self.recovery.recover(inst_id)
            except Exception as e:
                logger.error("Recovery failed for %s: %s", inst_id, e)

        self._loop.run_until_complete(self._connect_and_run())

    async def _connect_and_run(self) -> None:
        # 在连接成功后订阅
        # OKXWebSocketClient.connect 会在连接成功后重新订阅 _subscribed_args，
        # 但首次需要手动订阅。
        # 更简单：直接设置 subscribed_args 再 connect。
        args = [{"channel": "trades", "instId": i} for i in self.inst_ids]
        self.client._subscribed_args = args
        await self.client.connect()
