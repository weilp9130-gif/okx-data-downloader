"""OKX WebSocket 客户端连接管理"""

import asyncio
import json
import time
from typing import Callable, Dict, List, Optional

import websockets

from ..utils.logger import get_logger

logger = get_logger(__name__)

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


class OKXWebSocketClient:
    """OKX WebSocket 公共行情客户端

    负责连接、订阅、心跳、重连。
    """

    def __init__(
        self,
        url: str = OKX_WS_URL,
        on_message: Optional[Callable[[dict], None]] = None,
        ping_interval: int = 25,
        pong_timeout: int = 10,
        reconnect_delay: float = 1.0,
    ):
        self.url = url
        self.on_message = on_message
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.reconnect_delay = reconnect_delay

        self._ws = None
        self._running = False
        self._subscribed_args: List[dict] = []
        self._last_message_at = 0.0
        self._last_ping_at = 0.0
        self._last_pong_at = 0.0
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> None:
        """建立 WebSocket 连接并启动接收循环"""
        self._running = True
        while self._running:
            try:
                logger.info("Connecting to OKX WebSocket: %s", self.url)
                async with websockets.connect(self.url) as ws:
                    self._ws = ws
                    self._last_message_at = time.time()
                    # 重新订阅
                    if self._subscribed_args:
                        await self.subscribe(self._subscribed_args)
                    # 启动接收和心跳
                    await asyncio.gather(
                        self._receive_loop(),
                        self._heartbeat_loop(),
                    )
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning("WebSocket connection closed: %s", e)
            except Exception as e:
                logger.error("WebSocket error: %s", e)

            if not self._running:
                break
            # 退避重连
            delay = min(self.reconnect_delay * 2, 30)
            logger.info("Reconnecting in %.1fs...", delay)
            await asyncio.sleep(delay)

    async def subscribe(self, args: List[dict]) -> None:
        """发送订阅消息"""
        if not args:
            return
        # 合并已订阅参数，用于重连
        for a in args:
            if a not in self._subscribed_args:
                self._subscribed_args.append(a)
        if self._ws is None:
            return
        msg = {"op": "subscribe", "args": args}
        await self._ws.send(json.dumps(msg))
        logger.info("Subscribed: %s", args)

    def set_subscriptions(self, args: List[dict]) -> None:
        """设置连接建立后要订阅的参数列表（供 connect 重连时使用）"""
        self._subscribed_args = list(args)

    async def unsubscribe(self, args: List[dict]) -> None:
        """发送取消订阅消息"""
        if self._ws is None:
            return
        msg = {"op": "unsubscribe", "args": args}
        await self._ws.send(json.dumps(msg))
        for a in args:
            if a in self._subscribed_args:
                self._subscribed_args.remove(a)
        logger.info("Unsubscribed: %s", args)

    async def close(self) -> None:
        """关闭连接"""
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    async def _receive_loop(self) -> None:
        """接收消息循环"""
        while self._running:
            try:
                msg = await asyncio.wait_for(self._ws.recv(), timeout=self.ping_interval + self.pong_timeout)
                self._last_message_at = time.time()
                # OKX 服务器可能发送文本 "ping"，回复 "pong"
                if isinstance(msg, str) and msg == "ping":
                    await self._ws.send("pong")
                    continue
                # OKX 服务器对 ping 的响应可能是文本 "pong"
                if isinstance(msg, str) and msg == "pong":
                    self._last_pong_at = time.time()
                    continue
                data = json.loads(msg)
                # 处理 JSON 格式 pong
                if data.get("op") == "pong":
                    self._last_pong_at = time.time()
                    continue
                if self.on_message:
                    self.on_message(data)
            except asyncio.TimeoutError:
                logger.warning("WebSocket receive timeout")
            except websockets.exceptions.ConnectionClosed:
                break
            except json.JSONDecodeError as e:
                logger.warning("Failed to decode WS message: %s", e)

    async def _heartbeat_loop(self) -> None:
        """心跳循环"""
        while self._running:
            await asyncio.sleep(self.ping_interval)
            if self._ws is None:
                continue
            try:
                await self._ws.send("ping")
                self._last_ping_at = time.time()
            except Exception as e:
                logger.warning("Failed to send ping: %s", e)
                break
