"""延迟探针 - 指标聚合模块

- 分位数：线性插值（numpy 语义，纯 Python，零新依赖）
- jitter_ms：样本标准差 `sqrt(Σ(x-mean)^2/(n-1))`，`n<2 -> 0`；禁 pstdev
- WindowSummarizer：内存聚合 / 固定 UTC 窗口 / 延后一个窗口关闭 / sample_ts
  归属 / corrected 逐样本自身 offset / 负延迟检查
- StatsRegistry：线程安全 系统级计数器注册表（window_start -> metric -> value）
"""

import math
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

from ..utils.logger import get_logger

logger = get_logger(__name__)

# 字段长度校验（P0 决策 23）
MAX_SOURCE_LEN = 20
MAX_CHANNEL_LEN = 32
MAX_METRIC_LEN = 32
MAX_INST_ID_LEN = 50

# 非 WS 身份字段（P0-9）
SYSTEM_INST_ID = "__system__"
WS_CHANNEL = "__ws__"
HTTP_CHANNEL = "__http__"

SOURCE_OKX = "okx"

# 指标名（latency_samples 只存 raw；corrected 只在 summaries/SQL）
RAW_METRIC = "raw_ws_receive_latency"
CORRECTED_METRIC = "corrected_ws_receive_latency"
WS_PING_METRIC = "ws_ping_rtt"
HTTP_RTT_METRIC = "http_rtt"
STRATEGY_METRIC = "strategy_latency"
STRATEGY_FEATURE_METRIC = "strategy_feature_latency"
STRATEGY_MODEL_METRIC = "strategy_model_latency"

# 样本类别（P0-11 market 计数器范围）
SAMPLE_CLASS_MARKET = "market"
SAMPLE_CLASS_NON_MARKET = "non_market"

# 统计口径计数器名（latency_probe_stats）
STAT_MARKET_SAMPLES = "market_samples_received"
STAT_PARSE_ERRORS = "parse_errors"
STAT_SAMPLES_PARSED = "samples_parsed"
STAT_QUEUED = "queued"
STAT_DROPPED = "dropped"
STAT_WRITTEN = "written"
STAT_WRITE_ERRORS = "write_errors"
STAT_FINAL_FAILED = "final_write_failed_samples"
STAT_SEQ_GAP = "sequence_gap_count"
STAT_SEQ_RESET = "sequence_reset_count"
STAT_PING_MISSED = "ws_ping_missed"
STAT_NEG_RAW = "negative_raw_latency_count"
STAT_NEG_CORRECTED = "negative_corrected_latency_count"
STAT_HTTP_ERRORS = "http_probe_errors"
STAT_NOTICE = "notice_received"
STAT_STRATEGY_DROPPED = "strategy_events_dropped"
STAT_WS_CONNECT = "ws_connect_count"
STAT_WS_RECONNECT = "ws_reconnect_count"
STAT_WS_MESSAGES = "ws_messages_received"
# C-P1-6：WS 非行情消息分类（与 parse_errors 严格区分）
STAT_WS_CONTROL = "ws_control_messages"
STAT_WS_UNKNOWN = "ws_unknown_messages"
STAT_WS_PARSE_ERRORS = "ws_parse_errors"
# C-P1-3：WS 端点连接统计
STAT_WS_CONNECT_ATTEMPTS = "ws_connect_attempts"
STAT_WS_CONNECT_FAILURES = "ws_connect_failures"
STAT_WS_ENDPOINT_FALLBACKS = "ws_endpoint_fallbacks"

# C-P0-4：corrected < -10ms 额外限频 WARNING（每窗口至多一条）
NEGATIVE_CORRECTED_WARN_MS = -10.0


def window_start_for(ts: datetime, interval: int) -> datetime:
    """固定 UTC 窗口起点：`floor(sample_ts / interval) * interval`"""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    seconds = int(ts.timestamp()) // interval * interval
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def percentile(values: List[float], p: float) -> Optional[float]:
    """线性插值分位数（numpy.percentile linear 语义）。

    输入无需排序：numpy 语义先排序再插值（冒烟实测发现未排序输入会得到
    p50 > p95 的错误结果，故必须排序）。
    """
    if not values:
        return None
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n == 1:
        return float(sorted_values[0])
    rank = p / 100.0 * (n - 1)
    lower = int(math.floor(rank))
    upper = min(lower + 1, n - 1)
    weight = rank - lower
    return (float(sorted_values[lower]) * (1.0 - weight)
            + float(sorted_values[upper]) * weight)


def sample_stddev(values: List[float]) -> float:
    """样本标准差；n<2 -> 0（禁 statistics.pstdev）"""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return math.sqrt(sum((x - mean) ** 2 for x in values) / (n - 1))


def lag_percentiles(lags: List[float]) -> Dict[str, float]:
    """event-loop lag 序列的 p50/p95/p99/max（C-P1-2，log-only）。"""
    if not lags:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "p50": percentile(lags, 50) or 0.0,
        "p95": percentile(lags, 95) or 0.0,
        "p99": percentile(lags, 99) or 0.0,
        "max": max(lags),
    }


def build_summary_row(window_start, source, inst_id, channel, metric, values) -> dict:
    n = len(values)
    return {
        "window_start": window_start,
        "source": source,
        "inst_id": inst_id,
        "channel": channel,
        "metric": metric,
        "n": n,
        "min_ms": min(values),
        "mean_ms": sum(values) / n,
        "p50_ms": percentile(values, 50),
        "p95_ms": percentile(values, 95),
        "p99_ms": percentile(values, 99),
        "max_ms": max(values),
        "jitter_ms": sample_stddev(values),
    }


class StatsRegistry:
    """线程安全系统级计数器注册表。

    key = window_start (datetime, 已按 interval floor)；metric 为统计口径名。
    written 允许最终一致（P1-21）：已关闭窗口在 writer drain/停止前仍可接收
    迟到的 written 更新。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[datetime, Dict[str, int]] = {}
        self._known_windows: set = set()
        self._totals: Dict[str, int] = {}   # 全量累计（不随窗口裁剪，有界：仅 20 个口径）

    def add(self, window: datetime, updates: dict) -> None:
        if not updates:
            return
        with self._lock:
            self._known_windows.add(window)
            bucket = self._counters.setdefault(window, {})
            for k, v in updates.items():
                n = int(v)
                bucket[k] = bucket.get(k, 0) + n
                self._totals[k] = self._totals.get(k, 0) + n

    def increment(self, window: datetime, metric: str, n: int = 1) -> None:
        self.add(window, {metric: n})

    def snapshot(self, window: datetime) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters.get(window, {}))

    def windows(self) -> List[datetime]:
        with self._lock:
            return sorted(self._known_windows)

    def window_value(self, window: datetime, metric: str, default: int = 0) -> int:
        with self._lock:
            return self._counters.get(window, {}).get(metric, default)

    def prune(self, before: datetime) -> None:
        """裁剪早于 before 的窗口簿记（已发射窗口），限制长期运行内存增长。"""
        with self._lock:
            stale = [w for w in self._known_windows if w < before]
            for w in stale:
                self._known_windows.discard(w)
                self._counters.pop(w, None)

    def total(self, metric: str) -> int:
        with self._lock:
            return self._totals.get(metric, 0)

    def totals(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._totals)


class WindowAgg:
    """单个窗口的序列聚合（key: (metric, channel, inst_id) -> values）"""

    def __init__(self) -> None:
        self.series: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)


class WindowSummarizer:
    """内存窗口汇总器。

    - 窗口固定 UTC，按 sample_ts 归属（decision 14/15）
    - 窗口 W 在 now >= W + 2*interval 时关闭（延后一个窗口）
    - corrected 用样本自身 clock_offset_ms；窗口级校正禁止（P0）；
      `corrected = value - clock_offset_ms`（C-P0-3 符号约定统一）
    - 关闭时调用 on_close(summary_rows, stats_rows)（UPSERT 到 summaries/stats）
    """

    def __init__(
        self,
        interval_seconds: int,
        stats: StatsRegistry,
        source: str = SOURCE_OKX,
        on_close: Optional[Callable[[List[dict], List[dict]], None]] = None,
    ) -> None:
        self.interval = int(interval_seconds)
        self.stats = stats
        self.source = source
        self._on_close = on_close
        self._lock = threading.Lock()
        self._windows: Dict[datetime, WindowAgg] = {}
        self._closed_windows: set = set()
        self._neg_corrected_warned: set = set()   # C-P0-4：每窗口至多一条告警

    def add_sample(self, sample: dict) -> None:
        """sample: sample_ts / metric / channel / inst_id / value_ms /
        clock_offset_ms(可选)"""
        try:
            metric = sample["metric"]
            channel = sample["channel"]
            inst = sample["inst_id"]
            value = float(sample["value_ms"])
            w = window_start_for(sample["sample_ts"], self.interval)
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Summarizer add_sample rejected: %s (%r)", e, sample)
            return
        with self._lock:
            agg = self._windows.setdefault(w, WindowAgg())
            agg.series[(metric, channel, inst)].append(value)
            if metric == RAW_METRIC:
                offset = sample.get("clock_offset_ms")
                if value < 0:
                    self.stats.increment(w, STAT_NEG_RAW)
                if offset is not None:
                    corrected = value - offset          # C-P0-3: value - offset
                    agg.series[(CORRECTED_METRIC, channel, inst)].append(corrected)
                    if corrected < 0:
                        self.stats.increment(w, STAT_NEG_CORRECTED)
                    if (corrected < NEGATIVE_CORRECTED_WARN_MS
                            and w not in self._neg_corrected_warned):
                        self._neg_corrected_warned.add(w)
                        logger.warning(
                            "Negative corrected latency: raw=%.1fms "
                            "clock_offset=%.1fms corrected=%.1fms",
                            value, offset, corrected,
                        )

    def tick(self, now: datetime) -> None:
        """关闭满足延后关闭条件的窗口（含只有计数器的窗口）"""
        threshold = now - timedelta(seconds=2 * self.interval)
        closable = []
        with self._lock:
            for w in list(self._windows.keys()):
                if w <= threshold:
                    closable.append((w, self._windows.pop(w)))
        for w, agg in closable:
            self._emit_window(w, agg)
        for w in self.stats.windows():
            if w <= threshold and w not in self._closed_windows:
                self._emit_window(w, None)
        self._prune_bookkeeping(threshold)

    def flush_all(self) -> None:
        """停止时关闭所有剩余窗口"""
        with self._lock:
            items = list(self._windows.items())
            self._windows.clear()
        for w, agg in items:
            self._emit_window(w, agg)
        for w in self.stats.windows():
            if w not in self._closed_windows:
                self._emit_window(w, None)

    def _prune_bookkeeping(self, threshold: datetime) -> None:
        """裁剪已发射的过期窗口簿记（P1：长期运行内存/排序有界）。"""
        self.stats.prune(threshold)
        with self._lock:
            stale = [w for w in self._closed_windows if w < threshold]
            for w in stale:
                self._closed_windows.discard(w)
            stale_neg = [w for w in self._neg_corrected_warned if w < threshold]
            for w in stale_neg:
                self._neg_corrected_warned.discard(w)

    def is_window_closed(self, w: datetime) -> bool:
        with self._lock:
            return w in self._closed_windows

    def _emit_window(self, w: datetime, agg: Optional[WindowAgg]) -> None:
        summary_rows: List[dict] = []
        if agg is not None:
            for (metric, channel, inst), values in agg.series.items():
                if not values:
                    continue
                summary_rows.append(
                    build_summary_row(w, self.source, inst, channel, metric, values)
                )
        stats = self.stats.snapshot(w)
        stats_rows = [
            {"window_start": w, "source": self.source, "metric": m, "value": v}
            for m, v in sorted(stats.items())
        ]
        self._closed_windows.add(w)
        if self._on_close is not None:
            try:
                self._on_close(summary_rows, stats_rows)
            except Exception as e:
                logger.error("Window close callback failed: %s", e)
