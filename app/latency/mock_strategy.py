"""模拟策略负载（mock_strategy.py）

- `none`：无 worker，市场路径基线
- `light`：EMA(mid) + threshold(0.0001) + cooldown 0.5s -> strategy_latency
- `heavy`：固定工作负载 `heavy_v1`（~30 特征 + 固定计算路径）-> 
  strategy_feature_latency / strategy_model_latency / strategy_latency

策略在专用 worker（线程 + 有界队列）执行，绝不在 recv loop 同步；
worker 溢出计 strategy_events_dropped（>0 -> D 实验 DEGRADED）。

strategy_latency = signal_mono - arrival_mono（含 worker 排队，端到端）；
feature/model 阶段耗时之和 != strategy_latency（P1-18 允许计时容差）。
"""

import math
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from ..utils.logger import get_logger
from .metrics import (
    SAMPLE_CLASS_NON_MARKET,
    SOURCE_OKX,
    STAT_STRATEGY_DROPPED,
    STRATEGY_FEATURE_METRIC,
    STRATEGY_MODEL_METRIC,
    STRATEGY_METRIC,
    SYSTEM_INST_ID,
    window_start_for,
)

logger = get_logger(__name__)

WORKLOAD = "heavy_v1"
FEATURES_COUNT = 30
LIGHT_THRESHOLD = 0.0001
LIGHT_COOLDOWN = 0.5
STRATEGY_QUEUE_CAPACITY = 1000
STRATEGY_PUT_TIMEOUT = 0.1


class MockStrategy:
    """模拟策略：独立 worker 执行，用于 D 实验（接收路径 + 策略负载）。"""

    def __init__(
        self,
        mode: str,
        stats,
        summary_interval: int,
        on_sample: Optional[Callable[[dict], None]] = None,
        queue_capacity: int = STRATEGY_QUEUE_CAPACITY,
        put_timeout: float = STRATEGY_PUT_TIMEOUT,
    ) -> None:
        self.mode = mode
        self.stats = stats
        self.summary_interval = int(summary_interval)
        self.on_sample = on_sample
        self._queue_capacity = queue_capacity
        self.put_timeout = put_timeout
        self.workload = None
        self._stopped = threading.Event()
        self._queue: Optional[queue.Queue] = None
        self._thread: Optional[threading.Thread] = None
        self._ema: Optional[float] = None
        self._last_signal_mono = 0.0
        self._hist: list = []
        if mode != "none":
            self.workload = WORKLOAD if mode == "heavy" else None
            self._queue = queue.Queue(maxsize=queue_capacity)
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    # ------------------------------------------------------------------
    def queue_depth(self) -> int:
        return 0 if self._queue is None else self._queue.qsize()

    def queue_capacity(self) -> int:
        return self._queue_capacity if self._queue is not None else 0

    def submit(self, sample: dict) -> bool:
        """recv loop 提交行情事件；溢出计 strategy_events_dropped。"""
        if self.mode == "none":
            return True
        try:
            self._queue.put(sample, timeout=self.put_timeout)
            return True
        except queue.Full:
            w = window_start_for(datetime.now(timezone.utc), self.summary_interval)
            self.stats.increment(w, STAT_STRATEGY_DROPPED)
            logger.warning(
                "Strategy worker queue full, event dropped "
                "(strategy_events_dropped += 1; D run DEGRADED)"
            )
            return False

    def stop(self, timeout: float = 3.0) -> None:
        self._stopped.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                sample = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                if self.mode == "light":
                    self._process_light(sample)
                else:
                    self._process_heavy(sample)
            except Exception as e:
                logger.error("Strategy worker error: %s", e)

    def _mid(self, sample: dict) -> float:
        raw = sample.get("raw_item") or {}
        if sample.get("channel") == "trades":
            try:
                return float(raw.get("px"))
            except (TypeError, ValueError):
                return 0.0
        bids = raw.get("bids") or []
        asks = raw.get("asks") or []
        try:
            if bids and asks:
                return (float(bids[0][0]) + float(asks[0][0])) / 2.0
        except (TypeError, ValueError, IndexError):
            pass
        return 0.0

    # ------------------------------------------------------------------
    def _process_light(self, sample: dict) -> None:
        arrival_mono_ns = sample.get("arrival_mono_ns") or 0
        mid = self._mid(sample)
        if mid <= 0:
            return
        self._ema = mid if self._ema is None else self._ema * 0.9 + mid * 0.1
        now_mono = time.monotonic_ns()
        if self._last_signal_mono and (now_mono - self._last_signal_mono) / 1e9 < LIGHT_COOLDOWN:
            return
        if abs(self._ema - mid) / mid > LIGHT_THRESHOLD:
            self._last_signal_mono = now_mono
            self._emit(sample, STRATEGY_METRIC, (now_mono - arrival_mono_ns) / 1e6)

    def _process_heavy(self, sample: dict) -> None:
        arrival_mono_ns = sample.get("arrival_mono_ns") or 0
        f_start = time.monotonic_ns()
        vals = self._features(sample)
        f_end = time.monotonic_ns()
        feature_ms = (f_end - f_start) / 1e6
        m_start = time.monotonic_ns()
        self._model(vals)
        m_end = time.monotonic_ns()
        model_ms = (m_end - m_start) / 1e6
        strategy_ms = (m_end - arrival_mono_ns) / 1e6
        self._emit(sample, STRATEGY_FEATURE_METRIC, feature_ms)
        self._emit(sample, STRATEGY_MODEL_METRIC, model_ms)
        self._emit(sample, STRATEGY_METRIC, strategy_ms)

    def _features(self, sample: dict) -> list:
        """heavy_v1 固定计算路径：~30 特征（滑动窗口 + 三角函数）。"""
        price = self._mid(sample)
        self._hist.append(price)
        if len(self._hist) > 64:
            self._hist.pop(0)
        h = self._hist
        vals = []
        for i in range(FEATURES_COUNT):
            acc = 0.0
            start = max(0, len(h) - 8)
            for j in range(start, len(h)):
                acc += math.sin(h[j] * (i + 1) + j) * (j % 3 + 1)
            vals.append(acc)
        return vals

    def _model(self, vals: list) -> float:
        """固定 model 阶段计算（确定性）。"""
        acc = 0.0
        for i, v in enumerate(vals):
            acc += v * (i % 7 + 1) * 0.01
        return 1.0 / (1.0 + math.exp(-acc))

    # ------------------------------------------------------------------
    def _emit(self, sample: dict, metric: str, value_ms: float) -> None:
        now = datetime.now(timezone.utc)
        out = {
            "sample_class": SAMPLE_CLASS_NON_MARKET,
            "sample_ts": now,
            "recv_ts": now,
            "exchange_ts": None,
            "session": sample.get("session", 0),       # 触发频道所属 WS session
            "source": sample.get("source", SOURCE_OKX),
            "inst_id": sample.get("inst_id", SYSTEM_INST_ID),
            "channel": sample.get("channel", ""),       # 触发频道
            "metric": metric,
            "value_ms": value_ms,
            "clock_offset_ms": None,
        }
        if self.on_sample is not None:
            try:
                self.on_sample(out)
            except Exception as e:
                logger.error("Strategy sample callback failed: %s", e)
