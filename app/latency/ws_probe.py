"""OKX WebSocket 延迟探针（ws_probe.py）

- 独立 WS 客户端 + 唯一 recv_loop：`ws.recv()` 只能在 recv_loop 调用；
  ping loop 只 `ws.send("ping")`；pong/行情/ack/error/server ping/notice
  全部经同一 recv_loop 分发（decision 2）。
- `ws_messages_received` 仅在 `await ws.recv()` 成功返回应用层消息时 +1
  （P0-13）；ConnectionClosed / timeout / 协议错误 / cancellation 不计。
  RFC6455 控制帧 Ping/Pong 与 OKX 应用层文本 "ping"/"pong" 是两个层次，
  PingProbe 只测后者。
- seq 检测（P0-12）：仅 SEQ_CHANNELS；snapshot 只更新 last_seq_id、永不计
  gap/reset；update gap 优先、reset 仅在 prev 连续时；按 (session,channel,
  inst_id) 隔离；重连清空 seq 状态但**不重置 clock_offset**。
- 频道权限容错：订阅失败剔除不退出（不硬编码 VIP）；OKX notice -> 日志 +
  优雅重连（reconnect_reason=okx_notice）。
"""

import asyncio
import json
import os
import platform
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set, Tuple

import websockets

from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime
from .metrics import (
    MAX_CHANNEL_LEN,
    MAX_INST_ID_LEN,
    RAW_METRIC,
    SAMPLE_CLASS_MARKET,
    SAMPLE_CLASS_NON_MARKET,
    SOURCE_OKX,
    STAT_DROPPED,
    STAT_MARKET_SAMPLES,
    STAT_NOTICE,
    STAT_PARSE_ERRORS,
    STAT_PING_MISSED,
    STAT_QUEUED,
    STAT_SAMPLES_PARSED,
    STAT_SEQ_GAP,
    STAT_SEQ_RESET,
    STAT_WS_CONNECT,
    STAT_WS_CONNECT_ATTEMPTS,
    STAT_WS_CONNECT_FAILURES,
    STAT_WS_CONTROL,
    STAT_WS_ENDPOINT_FALLBACKS,
    STAT_WS_MESSAGES,
    STAT_WS_PARSE_ERRORS,
    STAT_WS_RECONNECT,
    STAT_WS_UNKNOWN,
    SYSTEM_INST_ID,
    WS_CHANNEL,
    WS_PING_METRIC,
    window_start_for,
)

logger = get_logger(__name__)

# OKX 公共行情 WS 端点：8443 为主、443 为备选（实测部分网络 8443 不可达）。
# 可用环境变量 OKX_WS_URL 显式指定单个端点（不启用回退）。
_OKX_WS_URL_ENV = os.getenv("OKX_WS_URL", "").strip()
if _OKX_WS_URL_ENV:
    WS_URLS = [_OKX_WS_URL_ENV]
else:
    WS_URLS = [
        "wss://ws.okx.com:8443/ws/v5/public",
        "wss://ws.okx.com:443/ws/v5/public",
    ]

OPEN_TIMEOUT = 8.0
RECV_TIMEOUT = 1.0            # recv 无消息超时（不算 ws_messages_received）
SUBSCRIBE_TIMEOUT = 5.0       # 订阅 ack 等待超时

# C-P1-5：重连有限指数退避 [1,2,4,8,16,30,30,...] + 均匀 jitter [0, 0.5*delay]
RECONNECT_BACKOFF = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
RECONNECT_JITTER_FRACTION = 0.5
RECONNECT_STABLE_RESET_S = 60.0   # 连接稳定运行 >=60s 后连续失败计数归零

# seq 检测频道（P0-12）：只有增量盘口频道参与序列异常计数
SEQ_CHANNELS = {"books", "books-l2-tbt", "books50-l2-tbt"}
NON_SEQ_CHANNELS = {"bbo-tbt", "books5"}

# 支持的行情频道（默认 trades,bbo-tbt；candles 不支持）
ALLOWED_CHANNELS = {
    "trades", "bbo-tbt", "books5", "books", "books-l2-tbt", "books50-l2-tbt",
    "mark-price", "index-tickers", "tickers",
}

# 重连原因（P1-10）
RECONNECT_OKX_NOTICE = "okx_notice"
RECONNECT_SOCKET_CLOSED = "socket_closed"
RECONNECT_TIMEOUT = "timeout"
RECONNECT_PROTOCOL_ERROR = "protocol_error"
RECONNECT_MANUAL_STOP = "manual_stop"
RECONNECT_SUBSCRIBE_FAILURE = "subscribe_failure"


@dataclass
class ParsedSample:
    channel: str
    inst_id: str
    ts_ms: int
    seq_id: Optional[int] = None
    prev_seq_id: Optional[int] = None
    raw: dict = field(default_factory=dict)


class SeqState:
    __slots__ = ("last_seq_id",)

    def __init__(self, last_seq_id: Optional[int] = None) -> None:
        self.last_seq_id = last_seq_id


def _int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def detect_market_sample(channel: str, item) -> bool:
    """频道特定检测（P0 决策 12）：只判形状，不判 ts/instId。

    bbo-tbt 检测 = bids/asks 两个 list（与 books5 同构，空 list 合法）。
    `"ts" in item` 作为唯一检测被禁止。
    """
    if not isinstance(item, dict):
        return False
    if channel == "trades":
        return "px" in item and "sz" in item
    if channel == "bbo-tbt":
        return isinstance(item.get("bids"), list) and isinstance(item.get("asks"), list)
    if channel in ("books5", "books", "books-l2-tbt", "books50-l2-tbt"):
        return isinstance(item.get("bids"), list) and isinstance(item.get("asks"), list)
    if channel == "mark-price":
        return "markPx" in item
    if channel == "index-tickers":
        return "idxPx" in item
    if channel == "tickers":
        return "last" in item
    return False


def parse_market_sample(channel: str, item: dict,
                        fallback_inst: Optional[str] = None) -> Tuple[Optional[ParsedSample], Optional[str]]:
    """解析最低要求：ts 可转合法毫秒 int + instId 转非空 str。

    实测（P1-24 协议冒烟）：bbo-tbt / books* 频道的 data 项**不含 instId**，
    instId 只存在于消息 arg 中——因此支持从 arg 传入 fallback_inst。
    parse_errors 只计 ts/instId 失败；缺 tradeId/side/seqId/prevSeqId 等
    字段永不计 parse_errors。
    """
    if not isinstance(item, dict):
        return None, "not a dict"
    ts_raw = item.get("ts")
    inst = item.get("instId")
    if inst is None and fallback_inst is not None:
        inst = fallback_inst
    if ts_raw is None:
        return None, "ts missing"
    try:
        ts_ms = int(ts_raw)
    except (ValueError, TypeError):
        return None, "invalid ts %r" % (ts_raw,)
    if not isinstance(inst, str) or not inst.strip():
        return None, "invalid instId %r" % (inst,)
    return ParsedSample(channel=channel, inst_id=inst, ts_ms=ts_ms), None


class PingProbe:
    """应用层文本 "ping" -> "pong" 探针（非 RFC6455 控制帧）。

    唯一 pending ping；`ws.send("ping")` 只在 ping loop 调用；
    pong 在 recv loop 经 on_pong() 解析。超时计 ws_ping_missed。
    """

    def __init__(self, ping_interval: float, ping_timeout: float) -> None:
        self.ping_interval = float(ping_interval)
        self.ping_timeout = float(ping_timeout)
        self._ping_id = 0
        self.pending: Optional[dict] = None   # {id, send_mono_ns, deadline_mono_ns}

    async def send(self, ws) -> None:
        self._ping_id += 1
        now = time.monotonic_ns()
        self.pending = {
            "id": self._ping_id,
            "send_mono_ns": now,
            "deadline_mono_ns": now + self.ping_timeout * 1e9,
        }
        await ws.send("ping")

    def on_pong(self) -> Optional[float]:
        """返回 rtt_ms；无 pending ping 的 stray pong 返回 None（DEBUG）。"""
        if self.pending is None:
            return None
        now = time.monotonic_ns()
        rtt_ms = (now - self.pending["send_mono_ns"]) / 1e6
        self.pending = None
        return rtt_ms

    def check_timeout(self) -> bool:
        """pending ping 超时未收到 pong -> True（调用方计 ws_ping_missed）。"""
        if self.pending is None:
            return False
        if time.monotonic_ns() > self.pending["deadline_mono_ns"]:
            self.pending = None
            return True
        return False


class WSProbe:
    """OKX WebSocket 延迟探针（唯一 recv_loop + PingProbe + 重连 + seq）。"""

    def __init__(
        self,
        insts: List[str],
        channels: List[str],
        ping_interval: float,
        ping_timeout: float,
        stats,
        summary_interval: int,
        clock,
        on_sample: Callable[[dict], bool],
        strategy=None,
        source: str = SOURCE_OKX,
        ws_connect_fn: Callable = websockets.connect,
    ) -> None:
        self.insts = list(insts)
        self.channels = list(channels)
        self.ping_interval = float(ping_interval)
        self.ping_timeout = float(ping_timeout)
        self.stats = stats
        self.summary_interval = int(summary_interval)
        self.clock = clock
        self.on_sample = on_sample
        self.strategy = strategy
        self.source = source
        self._connect_fn = ws_connect_fn

        self._ws = None
        self._running = False
        self._stop = asyncio.Event()
        self.session = 0                       # 1+ 真实 WS session
        self.conn_id: Optional[str] = None
        self._exit_reason: Optional[str] = None
        self._working_url: Optional[str] = None
        self.seq_states: Dict[Tuple[int, str, str], SeqState] = {}
        self.ping_probe = PingProbe(ping_interval, ping_timeout)
        self._pending_subs: Set[Tuple[str, str]] = set()
        self._active_subs: Set[Tuple[str, str]] = set()
        self._subscribe_done: asyncio.Event = asyncio.Event()
        # C-P1-5：重连退避状态（测试可注入 jitter/延迟）
        self._consecutive_failures = 0
        self._conn_up_mono: Optional[float] = None
        self._jitter_fn = random.uniform

    # ------------------------------------------------------------------
    def _now_window(self) -> datetime:
        return window_start_for(datetime.now(timezone.utc), self.summary_interval)

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    async def run(self) -> None:
        """主循环：连接 -> 订阅 -> recv/ping -> 按 reason 重连。"""
        self._running = True
        self._stop = asyncio.Event()
        while self._running and not self._stop.is_set():
            reason = await self._connect_once()
            if not self._running or self._stop.is_set():
                break
            if reason == RECONNECT_MANUAL_STOP:
                break
            if reason == RECONNECT_SUBSCRIBE_FAILURE:
                logger.error("All channel subscriptions failed; exiting")
                break
            self._maybe_reset_backoff()
            self._reset_for_reconnect()
            self.stats.increment(self._now_window(), STAT_WS_RECONNECT)
            gap_start = time.monotonic()
            await asyncio.sleep(self._next_reconnect_delay())
            gap_ms = (time.monotonic() - gap_start) * 1000.0
            logger.info("Reconnecting reason=%s connection_gap_ms=%.1f (log only)",
                        reason, gap_ms)
        self._running = False

    def _reset_for_reconnect(self) -> None:
        """重连：session += 1、清空 seq 状态、新 connId；**不重置 clock_offset**。"""
        self.seq_states.clear()

    def _next_reconnect_delay(self) -> float:
        """有限指数退避 + 均匀 jitter（C-P1-5）：[1,2,4,8,16,30,30,...]"""
        idx = min(self._consecutive_failures, len(RECONNECT_BACKOFF) - 1)
        base = RECONNECT_BACKOFF[idx]
        self._consecutive_failures += 1
        return base + self._jitter_fn(0.0, RECONNECT_JITTER_FRACTION * base)

    def _maybe_reset_backoff(self) -> None:
        """连接稳定运行 >=60s 后连续失败计数归零（C-P1-5）。"""
        if (self._conn_up_mono is not None
                and time.monotonic() - self._conn_up_mono >= RECONNECT_STABLE_RESET_S):
            self._consecutive_failures = 0
            logger.debug("Connection stable for >=%.0fs; reconnect backoff reset",
                         RECONNECT_STABLE_RESET_S)
        self._conn_up_mono = None

    # ------------------------------------------------------------------
    async def _connect_once(self) -> str:
        self.session += 1
        self.conn_id = uuid.uuid4().hex[:12]
        self.stats.increment(self._now_window(), STAT_WS_CONNECT)
        self._exit_reason = None
        self._pending_subs = {
            (ch, inst) for ch in self.channels for inst in self.insts
        }
        self._active_subs = set(self._pending_subs)
        self._subscribe_done = asyncio.Event()
        if self._working_url:
            urls = [self._working_url] + [u for u in WS_URLS if u != self._working_url]
        else:
            urls = WS_URLS
        for idx, url in enumerate(urls):
            if self._stop.is_set():
                break
            # 连接任务与 stop 事件竞争：shutdown 时立即取消在途连接，
            # 避免外层 wait_for 取消导致 attempts 欠计/停机卡住
            conn_task = asyncio.create_task(self._run_connection(url))
            stop_task = asyncio.create_task(self._stop.wait())
            done, pending = await asyncio.wait(
                {conn_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            for t in pending:
                try:
                    await t
                except BaseException:
                    pass
            self.stats.increment(self._now_window(), STAT_WS_CONNECT_ATTEMPTS)
            if conn_task in done:
                try:
                    reason = conn_task.result()
                except BaseException:
                    reason = "_connect_failed_"
                if reason == "_connect_failed_":
                    self.stats.increment(self._now_window(), STAT_WS_CONNECT_FAILURES)
                    if idx < len(urls) - 1:
                        self.stats.increment(self._now_window(),
                                             STAT_WS_ENDPOINT_FALLBACKS)
                        logger.warning(
                            "WS endpoint fallback from=%s to=%s reason=connect_failed",
                            url, urls[idx + 1],
                        )
                    continue
                self._working_url = url
                return reason
            # stop 在连接/订阅期间触发：该连接被取消，不视为端点失败
            return RECONNECT_MANUAL_STOP
        return RECONNECT_SOCKET_CLOSED

    async def _run_connection(self, url: str) -> str:
        try:
            ws = await self._connect_fn(
                url, ping_interval=None, ping_timeout=None, open_timeout=OPEN_TIMEOUT
            )
        except asyncio.TimeoutError:
            return "_connect_failed_"
        except websockets.exceptions.ConnectionClosed:
            return "_connect_failed_"
        except websockets.exceptions.InvalidHandshake:
            return "_connect_failed_"
        except Exception as e:
            logger.error("WS connect to %s error: %s", url, e)
            return "_connect_failed_"
        try:
            async with ws:
                self._ws = ws
                self._working_url = url       # C-P1-3：SESSION_START 记录 active_endpoint
                await self._send_subscribe(ws)
                recv_task = asyncio.create_task(self._recv_loop())
                ping_task = asyncio.create_task(self._ping_loop())
                try:
                    await asyncio.wait_for(self._subscribe_done.wait(),
                                           timeout=SUBSCRIBE_TIMEOUT)
                except asyncio.TimeoutError:
                    pass
                if not self._active_subs:
                    recv_task.cancel()
                    ping_task.cancel()
                    self._ws = None
                    return RECONNECT_SUBSCRIBE_FAILURE
                if self._stop.is_set():
                    # 关闭信号在订阅等待期间到达 -> 立即退出该连接，避免 shutdown
                    # 卡在 subscribe 等待、被外层 wait_for 取消导致 attempts 欠计
                    recv_task.cancel()
                    ping_task.cancel()
                    self._ws = None
                    return RECONNECT_MANUAL_STOP
                self._log_session_start()
                self._conn_up_mono = time.monotonic()   # C-P1-5：稳定运行计时起点
                done, pending = await asyncio.wait(
                    {recv_task, ping_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
                self._ws = None
                return self._exit_reason or RECONNECT_SOCKET_CLOSED
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WS connection closed (session=%d)", self.session)
            return RECONNECT_SOCKET_CLOSED
        except asyncio.TimeoutError:
            return RECONNECT_TIMEOUT
        except Exception as e:
            logger.error("WS error (session=%d): %s", self.session, e)
            return RECONNECT_PROTOCOL_ERROR

    async def _send_subscribe(self, ws) -> None:
        args = [
            {"channel": ch, "instId": inst}
            for ch in self.channels for inst in self.insts
        ]
        await ws.send(json.dumps({"op": "subscribe", "args": args}))

    def _log_session_start(self) -> None:
        offset = self.clock.active_offset()
        selected_rtt = None
        active_probe = getattr(self.clock, "active_probe", None)
        if active_probe is not None:
            p = active_probe()
            if p is not None:
                selected_rtt = p[0]     # C-P1-1：所选 min-RTT 探测的 rtt
        logger.info(
            "SESSION_START session=%d conn_id=%s start_ts=%s pid=%d python=%s "
            "platform=%s cpu_count=%s insts=%s channels=%s strategy=%s workload=%s "
            "ping_interval=%.2f ping_timeout=%.2f clock_offset_ms=%s "
            "selected_probe_rtt=%s active_endpoint=%s",
            self.session, self.conn_id, datetime.now(timezone.utc).isoformat(),
            os.getpid(), sys.version.split()[0], platform.platform(),
            os.cpu_count() or "?", ",".join(self.insts), ",".join(self.channels),
            getattr(self.strategy, "mode", "none") if self.strategy else "none",
            getattr(self.strategy, "workload", None) if self.strategy else None,
            self.ping_interval, self.ping_timeout,
            "%.2f" % offset if offset is not None else "NULL",
            "%.2f" % selected_rtt if selected_rtt is not None else "NULL",
            self._working_url or "",
        )

    # ------------------------------------------------------------------
    async def _recv_loop(self) -> None:
        """唯一 recv_loop：ws.recv() 只在此协程调用。"""
        while self._running and not self._stop.is_set():
            try:
                msg = await asyncio.wait_for(self._ws.recv(), timeout=RECV_TIMEOUT)
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                self._exit_reason = RECONNECT_SOCKET_CLOSED
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                self._exit_reason = RECONNECT_PROTOCOL_ERROR
                return
            # P0-13：仅成功返回应用层消息才 +1
            self.stats.increment(self._now_window(), STAT_WS_MESSAGES)
            recv_ns = time.time_ns()
            arrival_mono_ns = time.monotonic_ns()
            try:
                handled = await self._handle_message(msg, recv_ns, arrival_mono_ns)
            except websockets.exceptions.ConnectionClosed:
                self._exit_reason = RECONNECT_SOCKET_CLOSED
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                self._exit_reason = RECONNECT_PROTOCOL_ERROR
                return
            if not handled:
                return
        self._exit_reason = self._exit_reason or RECONNECT_MANUAL_STOP

    async def _handle_message(
        self, msg, recv_ns: int, arrival_mono_ns: int
    ) -> bool:
        """分发所有应用层消息；返回 False 表示退出 recv loop（触发重连）。

        注意：websockets 17 中文本帧（含 JSON 行情/ack）一律以 str 返回，
        JSON 解析须在 str 分支内进行。
        """
        if isinstance(msg, str):
            if msg == "ping":                       # 服务器文本 ping -> 回 pong
                self.stats.increment(self._now_window(), STAT_WS_CONTROL)
                try:
                    await self._ws.send("pong")
                except Exception:
                    self._exit_reason = RECONNECT_SOCKET_CLOSED
                    return False
                return True
            if msg == "pong":                       # 应用层 pong -> PingProbe
                self.stats.increment(self._now_window(), STAT_WS_CONTROL)
                self._handle_pong(recv_ns, arrival_mono_ns)
                return True
            try:
                data = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                self.stats.increment(self._now_window(), STAT_WS_PARSE_ERRORS)
                logger.debug("Unknown text WS message: %.200s", msg)
                return True
        elif isinstance(msg, (bytes, bytearray)):
            try:
                data = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                self.stats.increment(self._now_window(), STAT_WS_PARSE_ERRORS)
                logger.warning("Failed to decode binary WS message (protocol error)")
                return True
        else:
            logger.debug("Unsupported WS message type: %s", type(msg))
            return True
        if not isinstance(data, dict):
            return True
        event = data.get("event")
        op = data.get("op")
        if event == "subscribe":
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            self._handle_subscribe_ack(data)
            return True
        if event == "unsubscribe":
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            return True
        if event == "error":
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            self._handle_subscribe_error(data)
            return True
        if event == "notice":
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            logger.warning(
                "OKX notice: code=%s msg=%s connId=%s (graceful reconnect)",
                data.get("code"), data.get("msg"), data.get("connId"),
            )
            self.stats.increment(self._now_window(), STAT_NOTICE)
            self._exit_reason = RECONNECT_OKX_NOTICE
            return False
        if op == "pong":                            # JSON 形式 pong
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            self._handle_pong(recv_ns, arrival_mono_ns)
            return True
        if op == "ping":                            # 罕见 JSON ping
            self.stats.increment(self._now_window(), STAT_WS_CONTROL)
            try:
                await self._ws.send("pong")
            except Exception:
                self._exit_reason = RECONNECT_SOCKET_CLOSED
                return False
            return True
        if "arg" in data and isinstance(data.get("data"), list):
            self._handle_market_message(data, recv_ns, arrival_mono_ns)
            return True
        self.stats.increment(self._now_window(), STAT_WS_UNKNOWN)
        logger.debug("Unhandled WS message keys=%s", list(data.keys()))
        return True

    # ------------------------------------------------------------------
    def _handle_subscribe_ack(self, data: dict) -> None:
        arg = data.get("arg") or {}
        key = (arg.get("channel"), arg.get("instId"))
        code = data.get("code")
        msg = data.get("msg")
        self._pending_subs.discard(key)
        if key in self._active_subs and code not in (0, "0", None):
            logger.warning(
                "Subscribe failed channel=%s inst=%s code=%s msg=%s (channel dropped)",
                key[0], key[1], code, msg,
            )
            self._active_subs.discard(key)
        self._maybe_subscribe_done()

    def _handle_subscribe_error(self, data: dict) -> None:
        arg = data.get("arg") or {}
        key = (arg.get("channel"), arg.get("instId"))
        code = data.get("code")
        msg = data.get("msg")
        logger.warning(
            "Subscribe error channel=%s inst=%s code=%s msg=%s",
            key[0], key[1], code, msg,
        )
        self._pending_subs.discard(key)
        self._active_subs.discard(key)
        self._maybe_subscribe_done()

    def _maybe_subscribe_done(self) -> None:
        if not self._pending_subs:
            self._subscribe_done.set()

    # ------------------------------------------------------------------
    def _handle_pong(self, recv_ns: int, arrival_mono_ns: int) -> None:
        rtt_ms = self.ping_probe.on_pong()
        if rtt_ms is None:
            logger.debug("Stray pong (no pending ping)")
            return
        sample_ts = datetime.fromtimestamp(recv_ns / 1e9, tz=timezone.utc)
        sample = {
            "sample_class": SAMPLE_CLASS_NON_MARKET,
            "sample_ts": sample_ts,
            "recv_ts": sample_ts,
            "exchange_ts": None,
            "session": self.session,
            "source": self.source,
            "inst_id": SYSTEM_INST_ID,
            "channel": WS_CHANNEL,
            "metric": WS_PING_METRIC,
            "value_ms": rtt_ms,
            "clock_offset_ms": self.clock.active_offset(),
        }
        self._emit_sample(sample)

    async def _ping_loop(self) -> None:
        """只 ws.send("ping")，不调用 recv()。"""
        while self._running and not self._stop.is_set():
            await asyncio.sleep(self.ping_interval)
            if self.ping_probe.check_timeout():
                self.stats.increment(self._now_window(), STAT_PING_MISSED)
                logger.warning("Ping timed out after %.1fs (ws_ping_missed += 1)",
                               self.ping_timeout)
            if self._stop.is_set() or self._ws is None:
                break
            try:
                await self.ping_probe.send(self._ws)
            except websockets.exceptions.ConnectionClosed:
                self._exit_reason = RECONNECT_SOCKET_CLOSED
                return
            except Exception:
                self._exit_reason = RECONNECT_PROTOCOL_ERROR
                return

    # ------------------------------------------------------------------
    def _handle_market_message(self, data: dict, recv_ns: int, arrival_mono_ns: int) -> None:
        arg = data.get("arg") or {}
        channel = arg.get("channel")
        data_list = data.get("data") or []
        if not channel or not isinstance(data_list, list):
            return
        action = data.get("action")
        sample_ts = datetime.fromtimestamp(recv_ns / 1e9, tz=timezone.utc)
        sample_w = window_start_for(sample_ts, self.summary_interval)
        offset = self.clock.active_offset()
        arg_inst = arg.get("instId")     # bbo-tbt/books* 项内无 instId，取 arg
        for item in data_list:
            if not detect_market_sample(channel, item):
                continue
            self.stats.increment(sample_w, STAT_MARKET_SAMPLES)
            parsed, err = parse_market_sample(channel, item, fallback_inst=arg_inst)
            if err is not None:
                logger.debug("parse_errors += 1: %s", err)
                self.stats.increment(sample_w, STAT_PARSE_ERRORS)
                continue
            self._check_seq(action, channel, parsed, item, sample_w)
            raw_ms = (recv_ns - parsed.ts_ms * 1e6) / 1e6
            if len(channel) > MAX_CHANNEL_LEN or len(parsed.inst_id) > MAX_INST_ID_LEN:
                logger.error(
                    "Field-length violation; counted as parse_errors: %s/%s",
                    channel, parsed.inst_id,
                )
                self.stats.increment(sample_w, STAT_PARSE_ERRORS)
                continue
            sample = {
                "sample_class": SAMPLE_CLASS_MARKET,
                "sample_ts": sample_ts,
                "recv_ts": sample_ts,
                "exchange_ts": ms_to_datetime(parsed.ts_ms),
                "session": self.session,
                "source": self.source,
                "inst_id": parsed.inst_id,
                "channel": channel,
                "metric": RAW_METRIC,
                "value_ms": raw_ms,
                "clock_offset_ms": offset,
                "arrival_mono_ns": arrival_mono_ns,   # 内存专用，禁持久化
                "raw_item": item,
            }
            self._emit_sample(sample)
            self.stats.increment(sample_w, STAT_SAMPLES_PARSED)
            if self.strategy is not None:
                self.strategy.submit(sample)

    def _emit_sample(self, sample: dict) -> None:
        """sample 同时入 writer 队列 + 内存汇总器；market 才计 queued/dropped。"""
        ok = self.on_sample(sample)
        if sample.get("sample_class") == SAMPLE_CLASS_MARKET:
            w = window_start_for(sample["sample_ts"], self.summary_interval)
            if ok:
                self.stats.increment(w, STAT_QUEUED)
            else:
                self.stats.increment(w, STAT_DROPPED)

    # ------------------------------------------------------------------
    def _check_seq(self, action, channel: str, parsed: ParsedSample, item: dict,
                   w: datetime) -> None:
        """seq 连续性检测（P0-12）：snapshot 永不计 gap/reset；update 时
        gap 优先、reset 仅 prev 连续回退；无 action 不猜；无 prior state 不计。"""
        if channel not in SEQ_CHANNELS:
            return
        key = (self.session, channel, parsed.inst_id)
        if action == "snapshot":
            seq_id = _int_or_none(item.get("seqId"))
            if seq_id is not None:
                state = self.seq_states.setdefault(key, SeqState())
                state.last_seq_id = seq_id
            return
        if action == "update":
            state = self.seq_states.get(key)
            prev = _int_or_none(item.get("prevSeqId"))
            seq = _int_or_none(item.get("seqId"))
            if state is None:
                if seq is not None:
                    self.seq_states[key] = SeqState(last_seq_id=seq)
                return
            if prev is None or seq is None:
                return
            if prev != state.last_seq_id:
                self.stats.increment(w, STAT_SEQ_GAP)
                logger.warning(
                    "Sequence gap key=%s prev=%s last=%s (continuity break)",
                    key, prev, state.last_seq_id,
                )
            elif seq < prev:
                self.stats.increment(w, STAT_SEQ_RESET)
                logger.warning(
                    "Sequence reset key=%s prev=%s seq=%s (exchange reset/anomaly)",
                    key, prev, seq,
                )
            state.last_seq_id = seq
            return
        logger.debug("No action in %s message; seq not checked", channel)
