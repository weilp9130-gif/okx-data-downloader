"""时钟校准模块（REST server time based clock offset estimate）

- probe_once：NTP midpoint + 一次 GET /api/v5/public/time
  RTT 必须用 monotonic 时钟（C-P0-1，wall clock 受 NTP/改时影响）；
  `offset_ms = (t0_wall+t1_wall)/2 - server_ts_ms*1e6`（C-P0-3 统一符号：
  `clock_offset = local_time - server_time`，本机超前为正）。
- ClockOffsetTracker：启动串行探测（P1-11）；运行校准窗口只保留最近 20 个
  成功探测（P1-19）；min-RTT 成功探测的 offset 为 active offset；
  active offset 跳变 > 50ms 记 CLOCK_OFFSET_JUMP WARNING（C-P0-2，不拒绝新值）。
- 失败探测（P1-8）：http_probe_errors +1、不产生 http_rtt sample、不入窗、
  保留上次有效 offset；无有效校准则 clock_offset_ms 为 NULL 且不产 corrected。
"""

import asyncio
import time
import threading
from datetime import datetime, timezone
from typing import Callable, List, Optional, Tuple

import requests

from ..utils.logger import get_logger
from .metrics import (
    HTTP_CHANNEL,
    HTTP_RTT_METRIC,
    SAMPLE_CLASS_NON_MARKET,
    SOURCE_OKX,
    STAT_HTTP_ERRORS,
    SYSTEM_INST_ID,
    window_start_for,
)

logger = get_logger(__name__)

OKX_TIME_URL = "https://www.okx.com/api/v5/public/time"

CLOCK_WARNING_MS = 50.0
CLOCK_OFFSET_JUMP_WARNING_MS = 50.0   # active offset 跳变阈值（C-P0-2）
CALIBRATION_WINDOW_SIZE = 20          # 运行校准窗口只保留最近 20 个成功探测（P1-19）
STARTUP_PROBE_SPACING = 0.08          # 启动串行探测间隔 ~80ms（P1-11）


def probe_once(url: str = OKX_TIME_URL, timeout: float = 5.0) -> Optional[Tuple[float, float, int]]:
    """单次时钟探测。

    双时钟（C-P0-1）：`t0_mono/t1_mono` 只算 RTT；`t0_wall/t1_wall` 只算
    midpoint 与 offset。

    Returns:
        (rtt_ms, offset_ms, t1_wall_ns) 成功；失败返回 None
    """
    t0_mono = time.monotonic_ns()
    t0_wall = time.time_ns()
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        js = resp.json()
        server_ts_ms = int(js["data"][0]["ts"])
    except Exception:
        return None
    t1_mono = time.monotonic_ns()
    t1_wall = time.time_ns()
    rtt_ms = (t1_mono - t0_mono) / 1e6                     # RTT = monotonic duration
    midpoint_ns = (t0_wall + t1_wall) // 2                 # wall 只用于 midpoint
    offset_ms = (midpoint_ns - server_ts_ms * 1e6) / 1e6   # C-P0-3: local - server
    return rtt_ms, offset_ms, t1_wall


class ClockOffsetTracker:
    """时钟偏移跟踪器（min-RTT 选择 + 成功探测窗口）。"""

    def __init__(
        self,
        stats,
        summary_interval: int,
        source: str = SOURCE_OKX,
        window_size: int = CALIBRATION_WINDOW_SIZE,
        probe_fn: Callable = probe_once,
        on_sample: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self.stats = stats
        self.summary_interval = int(summary_interval)
        self.source = source
        self.window_size = window_size
        self.probe_fn = probe_fn
        self._on_sample = on_sample
        self._lock = threading.Lock()
        self._probes: List[Tuple[float, float, Optional[int]]] = []  # (rtt_ms, offset_ms, t1_ns)
        self._active_offset: Optional[float] = None
        self._offset_jumps = 0                        # C-P0-2 跳变计数（测试用）

    # ------------------------------------------------------------------
    def active_offset(self) -> Optional[float]:
        """当前 active clock offset（min-RTT 成功探测）；无有效校准则 None"""
        with self._lock:
            return self._active_offset

    def active_probe(self) -> Optional[Tuple[float, float, Optional[int]]]:
        """当前 active 探测记录 (rtt_ms, offset_ms, t1_ns)（C-P1-1）。"""
        with self._lock:
            if not self._probes:
                return None
            return min(self._probes, key=lambda p: p[0])

    def startup_calibrate(self, clock_probes: int) -> None:
        """启动校准：串行探测（P1-11），只收成功探测，~80ms 间隔。"""
        for _ in range(max(0, int(clock_probes))):
            self._run_probe_blocking()
            time.sleep(STARTUP_PROBE_SPACING)

    def _run_probe_blocking(self) -> None:
        result = self.probe_fn()
        if result is None:
            self._on_failure()
            return
        rtt_ms, offset_ms, t1_ns = result
        self._record_success(rtt_ms, offset_ms, t1_ns)
        self._emit_http_sample(t1_ns, rtt_ms, offset_ms)

    def _record_success(self, rtt_ms: float, offset_ms: float,
                        t1_ns: Optional[int] = None) -> None:
        jump: Optional[Tuple[float, float, float]] = None
        with self._lock:
            self._probes.append((rtt_ms, offset_ms, t1_ns))
            if len(self._probes) > self.window_size:
                self._probes = self._probes[-self.window_size:]
            best = min(self._probes, key=lambda p: p[0], default=None)
            if best is not None:
                old = self._active_offset
                self._active_offset = best[1]
                # C-P0-2：active offset 跳变检测（不拒绝新 offset，仅告警）
                if old is not None and abs(self._active_offset - old) > CLOCK_OFFSET_JUMP_WARNING_MS:
                    self._offset_jumps += 1
                    jump = (old, self._active_offset, best[0])
        if jump is not None:
            logger.warning(
                "CLOCK_OFFSET_JUMP old_offset=%.2f new_offset=%.2f delta_ms=%.2f "
                "selected_probe_rtt=%.2f",
                jump[0], jump[1], jump[1] - jump[0], jump[2],
            )
        if abs(offset_ms) > CLOCK_WARNING_MS:
            logger.warning(
                "Clock offset |%.1fms| > %.0fms (REST server time based estimate)",
                offset_ms, CLOCK_WARNING_MS,
            )

    def _on_failure(self) -> None:
        """失败探测（P1-8）：计 http_probe_errors；不产生 sample；不入窗；
        保留上次有效 offset；不改 active min-RTT。"""
        w = window_start_for(datetime.now(timezone.utc), self.summary_interval)
        self.stats.increment(w, STAT_HTTP_ERRORS)
        logger.warning("REST clock/RTT probe failed (http_probe_errors += 1)")

    def _emit_http_sample(self, t1_ns: int, rtt_ms: float, offset_ms: float) -> None:
        """http_rtt sample：sample_ts = t1（REST 完成时刻），recv_ts = sample_ts
        （P1-7）；clock_offset_ms 仅诊断，不参与值、不产 corrected（P1-20）。"""
        sample_ts = datetime.fromtimestamp(t1_ns / 1e9, tz=timezone.utc)
        sample = {
            "sample_class": SAMPLE_CLASS_NON_MARKET,
            "sample_ts": sample_ts,
            "recv_ts": sample_ts,
            "exchange_ts": None,
            "session": 0,                      # 非 WS（P0-9）
            "source": self.source,
            "inst_id": SYSTEM_INST_ID,
            "channel": HTTP_CHANNEL,
            "metric": HTTP_RTT_METRIC,
            "value_ms": rtt_ms,
            "clock_offset_ms": offset_ms,      # 仅诊断
        }
        if self._on_sample is not None:
            try:
                self._on_sample(sample)
            except Exception as e:
                logger.error("http_rtt sample callback failed: %s", e)

    # ------------------------------------------------------------------
    async def runtime_loop(self, interval_seconds: float,
                           stop_event: threading.Event) -> None:
        """周期探测（在线程池执行，绝不阻塞 recv loop）；interval<=0 禁用。"""
        if interval_seconds <= 0:
            return
        while not stop_event.is_set():
            await asyncio.sleep(interval_seconds)
            if stop_event.is_set():
                break
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._run_probe_blocking)
