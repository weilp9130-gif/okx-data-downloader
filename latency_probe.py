"""OKX 行情延迟探针入口（latency_probe.py）

持续测量 OKX WebSocket 行情到达本机的延迟分布，长期入库（PostgreSQL），
按窗口产出百分位汇总，用于快节奏量化前的延迟画像与昼夜/尖峰分析。

与现有实时采集管线完全独立（不改 sync_realtime.py / okx_ws.py / manager.py）。

示例：
    python latency_probe.py --insts BTC-USDT-SWAP --channels trades,bbo-tbt \
        --duration 300 --summary-interval 60 --ping-interval 1 --ping-timeout 5 \
        --http-rtt-interval 30 --clock-probes 20 --strategy-benchmark none
"""

import argparse
import asyncio
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.database import get_engine, init_db
from app.latency.clock import ClockOffsetTracker
from app.latency.metrics import (
    MAX_CHANNEL_LEN,
    MAX_INST_ID_LEN,
    SOURCE_OKX,
    STAT_DROPPED,
    STAT_FINAL_FAILED,
    STAT_HTTP_ERRORS,
    STAT_MARKET_SAMPLES,
    STAT_NEG_CORRECTED,
    STAT_NEG_RAW,
    STAT_NOTICE,
    STAT_PARSE_ERRORS,
    STAT_PING_MISSED,
    STAT_QUEUED,
    STAT_SAMPLES_PARSED,
    STAT_SEQ_GAP,
    STAT_SEQ_RESET,
    STAT_STRATEGY_DROPPED,
    STAT_WS_CONNECT,
    STAT_WS_CONNECT_ATTEMPTS,
    STAT_WS_CONNECT_FAILURES,
    STAT_WS_CONTROL,
    STAT_WS_ENDPOINT_FALLBACKS,
    STAT_WS_MESSAGES,
    STAT_WS_PARSE_ERRORS,
    STAT_WS_RECONNECT,
    STAT_WS_UNKNOWN,
    STAT_WRITE_ERRORS,
    STAT_WRITTEN,
    StatsRegistry,
    WindowSummarizer,
    lag_percentiles,
)
from app.latency.mock_strategy import MockStrategy, WORKLOAD
from app.latency.persistence import LatencySampleWriter, upsert_stats, upsert_summaries
from app.latency.ws_probe import ALLOWED_CHANNELS, WSProbe
from app.utils.logger import get_logger, setup_logging

logger = get_logger("latency_probe")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OKX 行情延迟探针（latency_probe）：WS/HTTP 延迟分布 + 窗口百分位汇总",
    )
    parser.add_argument("--insts", default="BTC-USDT-SWAP",
                        help="产品ID，逗号分隔，如 BTC-USDT-SWAP,ETH-USDT-SWAP")
    parser.add_argument("--channels", default="trades,bbo-tbt",
                        help="订阅频道，逗号分隔。可选: trades/bbo-tbt/books5/books/"
                             "books-l2-tbt/books50-l2-tbt/mark-price/index-tickers/tickers")
    parser.add_argument("--duration", type=int, default=300,
                        help="运行时长（秒），默认 300；0 = 无限")
    parser.add_argument("--summary-interval", type=int, default=60,
                        help="汇总窗口（秒），固定 UTC + 延后一个窗口关闭")
    parser.add_argument("--ping-interval", type=float, default=1.0,
                        help="PingProbe 发送间隔（秒）")
    parser.add_argument("--ping-timeout", type=float, default=None,
                        help="pending ping 超时（秒），默认 max(3*ping_interval, 5)")
    parser.add_argument("--http-rtt-interval", type=float, default=30.0,
                        help="REST 时钟/延迟探测间隔（秒）；0 = 禁用周期探测")
    parser.add_argument("--clock-probes", type=int, default=20,
                        help="启动串行时钟校准探测次数；0 = 禁用（无 corrected）")
    parser.add_argument("--strategy-benchmark", choices=["none", "light", "heavy"],
                        default="none",
                        help="模拟策略负载：none/light/heavy（heavy_v1，独立 worker）")
    return parser


def _validate(args) -> None:
    errors: List[str] = []
    insts = [i.strip() for i in args.insts.split(",") if i.strip()]
    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    if not insts:
        errors.append("--insts 不能为空")
    for i in insts:
        if len(i) > MAX_INST_ID_LEN:
            errors.append(f"inst_id 超长({len(i)}>{MAX_INST_ID_LEN}): {i}")
    if not channels:
        errors.append("--channels 不能为空")
    for c in channels:
        if c not in ALLOWED_CHANNELS:
            errors.append(f"不支持的频道: {c}（可选: {sorted(ALLOWED_CHANNELS)}）")
        if len(c) > MAX_CHANNEL_LEN:
            errors.append(f"channel 超长({len(c)}>{MAX_CHANNEL_LEN}): {c}")
    if args.summary_interval <= 0:
        errors.append("--summary-interval 必须 > 0")
    if args.ping_interval <= 0:
        errors.append("--ping-interval 必须 > 0")
    if errors:
        for e in errors:
            logger.error(e)
        sys.exit(2)


def _make_emit(writer: LatencySampleWriter, summarizer: WindowSummarizer):
    def emit(sample: dict) -> bool:
        ok = writer.put(sample)
        try:
            summarizer.add_sample(sample)
        except Exception as e:
            logger.error("Summarizer add_sample failed: %s", e)
        return ok
    return emit


class EventLoopLagMonitor:
    """event-loop lag 诊断（C-P1-2，log-only）：周期任务记录
    `lag_ms = 实际唤醒 - 期望唤醒`；不落库、不改任何 latency 计算。

    若日后 WS P99 高而 lag 也高 -> 本机 event-loop 主导；lag 正常而 WS P99
    高 -> 网络问题。
    """

    MAX_LAGS = 600

    def __init__(self, interval: float = 0.05, max_lags: int = MAX_LAGS) -> None:
        self.interval = float(interval)
        self._lags: deque = deque(maxlen=max_lags)

    async def run(self, stop_event: threading.Event) -> None:
        expected = time.monotonic()
        while not stop_event.is_set():
            await asyncio.sleep(self.interval)
            if stop_event.is_set():
                break
            actual = time.monotonic()
            self._lags.append((actual - expected) * 1000.0)
            expected = actual

    def summary(self) -> Dict[str, float]:
        return lag_percentiles(list(self._lags))

    def reset(self) -> None:
        self._lags.clear()


def _print_window(summary_rows: List[dict], stats_rows: List[dict]) -> None:
    if summary_rows:
        logger.info("=== 窗口百分位汇总（sample_ts 归属，延后关闭）===")
        header = ("window_start | channel | metric | n | min | p50 | p95 | p99 | "
                  "max | jitter")
        logger.info(header)
        for r in summary_rows:
            logger.info(
                "%s | %s | %s | %d | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f",
                r["window_start"].strftime("%H:%M:%S"), r["channel"], r["metric"],
                r["n"], r["min_ms"], r["p50_ms"], r["p95_ms"], r["p99_ms"],
                r["max_ms"], r["jitter_ms"],
            )
            # 阈值告警（P1-6）
            if r["metric"] not in ("raw_ws_receive_latency",
                                   "corrected_ws_receive_latency"):
                continue
            if r["p99_ms"] > 200.0:
                logger.error("THRESHOLD p99=%.1fms > 200ms (%s/%s)",
                             r["p99_ms"], r["channel"], r["metric"])
            elif r["p99_ms"] > 100.0:
                logger.warning("THRESHOLD p99=%.1fms > 100ms (%s/%s)",
                               r["p99_ms"], r["channel"], r["metric"])
    if stats_rows:
        s = {r["metric"]: r["value"] for r in stats_rows}
        parsed = s.get(STAT_SAMPLES_PARSED, 0)
        drop_rate = s.get(STAT_DROPPED, 0) / parsed if parsed else 0.0
        logger.info(
            "STATS msgs=%s mkt=%s parsed=%s queued=%s dropped=%s written=%s "
            "write_err=%s final_fail=%s parse_err=%s drop_rate=%.4f "
            "seq_gap=%s seq_reset=%s ping_missed=%s neg_raw=%s neg_corr=%s "
            "http_err=%s notice=%s strat_drop=%s conn=%s recon=%s "
            "ws_ctrl=%s ws_unknown=%s ws_parse_err=%s conn_attempts=%s "
            "conn_fails=%s fallbacks=%s",
            s.get(STAT_WS_MESSAGES, 0), s.get(STAT_MARKET_SAMPLES, 0),
            parsed, s.get(STAT_QUEUED, 0), s.get(STAT_DROPPED, 0),
            s.get(STAT_WRITTEN, 0), s.get(STAT_WRITE_ERRORS, 0),
            s.get(STAT_FINAL_FAILED, 0), s.get(STAT_PARSE_ERRORS, 0), drop_rate,
            s.get(STAT_SEQ_GAP, 0), s.get(STAT_SEQ_RESET, 0),
            s.get(STAT_PING_MISSED, 0), s.get(STAT_NEG_RAW, 0),
            s.get(STAT_NEG_CORRECTED, 0), s.get(STAT_HTTP_ERRORS, 0),
            s.get(STAT_NOTICE, 0), s.get(STAT_STRATEGY_DROPPED, 0),
            s.get(STAT_WS_CONNECT, 0), s.get(STAT_WS_RECONNECT, 0),
            s.get(STAT_WS_CONTROL, 0), s.get(STAT_WS_UNKNOWN, 0),
            s.get(STAT_WS_PARSE_ERRORS, 0), s.get(STAT_WS_CONNECT_ATTEMPTS, 0),
            s.get(STAT_WS_CONNECT_FAILURES, 0), s.get(STAT_WS_ENDPOINT_FALLBACKS, 0),
        )
        # 阈值告警（P1-6）
        if drop_rate > 0.01:
            logger.error("THRESHOLD drop_rate=%.2f%% > 1%%", drop_rate * 100.0)
        if s.get(STAT_SEQ_GAP, 0) > 0:
            logger.warning("THRESHOLD sequence_gap_count=%s > 0", s.get(STAT_SEQ_GAP))
        if s.get(STAT_NEG_RAW, 0) > 0 or s.get(STAT_NEG_CORRECTED, 0) > 0:
            logger.warning("THRESHOLD negative latency: raw=%s corrected=%s",
                           s.get(STAT_NEG_RAW, 0), s.get(STAT_NEG_CORRECTED, 0))


def _on_close(engine, summary_rows: List[dict], stats_rows: List[dict]) -> None:
    try:
        upsert_summaries(engine, summary_rows)
        upsert_stats(engine, stats_rows)
    except Exception as e:
        logger.error("Summary/stats UPSERT failed: %s", e)
    _print_window(summary_rows, stats_rows)


async def _ticker(summarizer: WindowSummarizer, stats: StatsRegistry,
                  writer: LatencySampleWriter, strategy: MockStrategy,
                  summary_interval: int, stop_ev: threading.Event,
                  lag_monitor: Optional[EventLoopLagMonitor] = None) -> None:
    """每秒 tick（关闭窗口）；每 summary_interval 输出队列/CPU/event-loop lag
    遥测（P1-23 / C-P1-2）。"""
    last_telemetry = time.monotonic()   # 避免首个 tick 立即触发遥测
    cpu_base = time.process_time()
    wall_base = time.monotonic()
    while not stop_ev.is_set():
        await asyncio.sleep(1.0)
        try:
            summarizer.tick(datetime.now(timezone.utc))
        except Exception as e:
            logger.error("Summarizer tick failed: %s", e)
        now_mono = time.monotonic()
        if now_mono - last_telemetry >= summary_interval:
            last_telemetry = now_mono
            cpu_now = time.process_time()
            wall_now = time.monotonic()
            cpu_pct = ((cpu_now - cpu_base) / (wall_now - wall_base) * 100.0
                       if wall_now > wall_base else 0.0)
            cpu_base, wall_base = cpu_now, wall_now
            if lag_monitor is not None:
                lag = lag_monitor.summary()
                logger.info(
                    "EVENT_LOOP_LAG p50=%.2f p95=%.2f p99=%.2f max=%.2f (ms)",
                    lag["p50"], lag["p95"], lag["p99"], lag["max"],
                )
            logger.info(
                "QUEUE_TELEMETRY writer_depth=%d writer_capacity=%d "
                "strategy_depth=%d strategy_capacity=%d cpu_time=%.3f "
                "cpu_percent=%.1f",
                writer.queue_depth(), writer.queue_capacity(),
                strategy.queue_depth(), strategy.queue_capacity(),
                cpu_now, cpu_pct,
            )


async def _run(args, insts: List[str], channels: List[str], ping_timeout: float) -> None:
    engine = get_engine()
    stats = StatsRegistry()
    summarizer = WindowSummarizer(
        args.summary_interval, stats,
        on_close=lambda srows, strows: _on_close(engine, srows, strows),
    )
    writer = LatencySampleWriter(stats, args.summary_interval)
    emit = _make_emit(writer, summarizer)
    strategy = MockStrategy(args.strategy_benchmark, stats, args.summary_interval,
                            on_sample=emit)
    clock = ClockOffsetTracker(stats, args.summary_interval, on_sample=emit)

    # 启动时钟校准：串行（P1-11）
    if args.clock_probes > 0:
        logger.info("Startup clock calibration: %d sequential probes", args.clock_probes)
        await asyncio.get_running_loop().run_in_executor(
            None, clock.startup_calibrate, args.clock_probes)
    offset = clock.active_offset()
    logger.info("Initial clock_offset (REST server time based estimate): %s ms",
                "%.2f" % offset if offset is not None else "NULL")

    probe = WSProbe(
        insts, channels, args.ping_interval, ping_timeout, stats,
        args.summary_interval, clock, on_sample=emit, strategy=strategy,
    )

    stop_ev = threading.Event()

    def _signal_handler(signum, frame):
        logger.warning("Signal received (%s), graceful stop...", signum)
        stop_ev.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    probe_task = asyncio.create_task(probe.run())
    http_task = asyncio.create_task(
        clock.runtime_loop(args.http_rtt_interval, stop_ev))
    lag_monitor = EventLoopLagMonitor()
    lag_task = asyncio.create_task(lag_monitor.run(stop_ev))
    ticker_task = asyncio.create_task(
        _ticker(summarizer, stats, writer, strategy, args.summary_interval,
                stop_ev, lag_monitor=lag_monitor))

    start = time.monotonic()
    try:
        while not stop_ev.is_set():
            if args.duration > 0 and time.monotonic() - start >= args.duration:
                logger.info("Duration reached (%.0fs), stopping...", args.duration)
                break
            await asyncio.sleep(0.2)
    finally:
        stop_ev.set()
        logger.info("Shutting down...")
        probe.stop()
        try:
            await asyncio.wait_for(probe_task, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        http_task.cancel()
        try:
            await http_task
        except asyncio.CancelledError:
            pass
        strategy.stop()
        ticker_task.cancel()
        try:
            await ticker_task
        except asyncio.CancelledError:
            pass
        lag_task.cancel()
        try:
            await lag_task
        except asyncio.CancelledError:
            pass
        lag = lag_monitor.summary()
        logger.info(
            "EVENT_LOOP_LAG (final) p50=%.2f p95=%.2f p99=%.2f max=%.2f (ms)",
            lag["p50"], lag["p95"], lag["p99"], lag["max"],
        )
        summarizer.flush_all()
        writer.stop(timeout=15.0)
        logger.info("===== 停止统计（全部窗口合计）=====")
        totals = stats.totals()
        parsed = totals.get(STAT_SAMPLES_PARSED, 0)
        drop_rate = totals.get(STAT_DROPPED, 0) / parsed if parsed else 0.0
        logger.info(
            "TOTAL msgs=%s mkt=%s parsed=%s queued=%s dropped=%s written=%s "
            "write_err=%s final_fail=%s parse_err=%s drop_rate=%.4f "
            "seq_gap=%s seq_reset=%s ping_missed=%s neg_raw=%s neg_corr=%s "
            "http_err=%s notice=%s strat_drop=%s conn=%s recon=%s "
            "ws_ctrl=%s ws_unknown=%s ws_parse_err=%s conn_attempts=%s "
            "conn_fails=%s fallbacks=%s",
            totals.get(STAT_WS_MESSAGES, 0), totals.get(STAT_MARKET_SAMPLES, 0),
            parsed, totals.get(STAT_QUEUED, 0), totals.get(STAT_DROPPED, 0),
            totals.get(STAT_WRITTEN, 0), totals.get(STAT_WRITE_ERRORS, 0),
            totals.get(STAT_FINAL_FAILED, 0), totals.get(STAT_PARSE_ERRORS, 0),
            drop_rate, totals.get(STAT_SEQ_GAP, 0), totals.get(STAT_SEQ_RESET, 0),
            totals.get(STAT_PING_MISSED, 0), totals.get(STAT_NEG_RAW, 0),
            totals.get(STAT_NEG_CORRECTED, 0), totals.get(STAT_HTTP_ERRORS, 0),
            totals.get(STAT_NOTICE, 0), totals.get(STAT_STRATEGY_DROPPED, 0),
            totals.get(STAT_WS_CONNECT, 0), totals.get(STAT_WS_RECONNECT, 0),
            totals.get(STAT_WS_CONTROL, 0), totals.get(STAT_WS_UNKNOWN, 0),
            totals.get(STAT_WS_PARSE_ERRORS, 0),
            totals.get(STAT_WS_CONNECT_ATTEMPTS, 0),
            totals.get(STAT_WS_CONNECT_FAILURES, 0),
            totals.get(STAT_WS_ENDPOINT_FALLBACKS, 0),
        )
        logger.info("latency_probe 已停止")


def main() -> int:
    setup_logging(name="latency_probe")
    parser = _build_parser()
    args = parser.parse_args()
    _validate(args)
    insts = [i.strip() for i in args.insts.split(",") if i.strip()]
    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    ping_timeout = (args.ping_timeout
                    if args.ping_timeout is not None
                    else max(3 * args.ping_interval, 5.0))
    logger.info(
        "latency_probe 启动 insts=%s channels=%s duration=%ds summary_interval=%ds "
        "ping_interval=%.2fs ping_timeout=%.2fs http_rtt_interval=%.1fs "
        "clock_probes=%d strategy=%s",
        insts, channels, args.duration, args.summary_interval,
        args.ping_interval, ping_timeout, args.http_rtt_interval,
        args.clock_probes, args.strategy_benchmark,
    )
    if args.strategy_benchmark == "heavy":
        logger.info("Strategy workload: %s", WORKLOAD)
    init_db()
    asyncio.run(_run(args, insts, channels, ping_timeout))
    return 0


if __name__ == "__main__":
    sys.exit(main())
