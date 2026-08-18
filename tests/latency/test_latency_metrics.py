"""latency_probe 离线单元测试（不依赖网络/数据库）

覆盖 B.13.1 / P0 / P1 的验证要点：
corrected 符号 / 时钟漂移（错误实现必须失败）/ corrected 不落表 / bbo-tbt
检测 / detect/parse 拆分 / seq 规则（snapshot 永不计、update gap 优先、reset
仅 prev 连续）/ 身份字段 / market 计数器范围 / ws_messages_received 边界 /
ping 配对 / 重连 seq 隔离（clock offset 不重置）/ channel 检测 / parse 最低
要求 / sample_ts 一致 / flush 状态机 / written 跨窗口拆分 / dropped vs
write_errors / jitter n<2->0 / strategy 容差 / HTTP 探测失败 / 启动串行 /
written 最终一致。
"""

import asyncio
import json
import threading
import time
import unittest
from datetime import datetime, timezone

import websockets

import app.latency.ws_probe as ws_probe_mod
from app.latency.clock import ClockOffsetTracker
from app.latency.metrics import (
    CORRECTED_METRIC,
    HTTP_CHANNEL,
    HTTP_RTT_METRIC,
    RAW_METRIC,
    SAMPLE_CLASS_MARKET,
    SAMPLE_CLASS_NON_MARKET,
    STAT_DROPPED,
    STAT_HTTP_ERRORS,
    STAT_FINAL_FAILED,
    STAT_MARKET_SAMPLES,
    STAT_NEG_CORRECTED,
    STAT_NEG_RAW,
    STAT_PARSE_ERRORS,
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
    STAT_WS_UNKNOWN,
    STAT_WRITE_ERRORS,
    STAT_WRITTEN,
    STRATEGY_FEATURE_METRIC,
    STRATEGY_MODEL_METRIC,
    STRATEGY_METRIC,
    SYSTEM_INST_ID,
    WS_CHANNEL,
    WS_PING_METRIC,
    StatsRegistry,
    WindowSummarizer,
    lag_percentiles,
    percentile,
    sample_stddev,
    window_start_for,
)
from app.latency.mock_strategy import MockStrategy
from app.latency.persistence import LatencySampleWriter, market_written_by_window
from app.latency.ws_probe import (
    ParsedSample,
    PingProbe,
    WSProbe,
    detect_market_sample,
    parse_market_sample,
)

UTC = timezone.utc


def now_ms() -> int:
    return int(time.time() * 1000)


def utcnow() -> datetime:
    return datetime.now(UTC)


def mk_market_sample(value_ms=10.0, inst="BTC-USDT-SWAP", channel="trades",
                     session=1, ts=None, offset=None):
    ts = ts or utcnow()
    return {
        "sample_class": "market",
        "sample_ts": ts,
        "recv_ts": ts,
        "exchange_ts": ts,
        "session": session,
        "source": "okx",
        "inst_id": inst,
        "channel": channel,
        "metric": RAW_METRIC,
        "value_ms": value_ms,
        "clock_offset_ms": offset,
        "arrival_mono_ns": time.monotonic_ns(),
        "raw_item": {"px": "100", "sz": "1"},
    }


def mk_ping_sample(session=1, ts=None):
    ts = ts or utcnow()
    return {
        "sample_class": "non_market",
        "sample_ts": ts,
        "recv_ts": ts,
        "exchange_ts": None,
        "session": session,
        "source": "okx",
        "inst_id": SYSTEM_INST_ID,
        "channel": WS_CHANNEL,
        "metric": WS_PING_METRIC,
        "value_ms": 3.0,
        "clock_offset_ms": None,
    }


class FakeClock:
    def __init__(self, offset=None):
        self._offset = offset

    def active_offset(self):
        return self._offset


class FakeWS:
    def __init__(self, messages=None):
        self._messages = list(messages or [])
        self.sent = []

    async def recv(self):
        if self._messages:
            return self._messages.pop(0)
        raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)

    async def send(self, data):
        self.sent.append(data)


class FakeWSHang:
    async def recv(self):
        await asyncio.sleep(30)

    async def send(self, data):
        pass


class FakeConn:
    def __init__(self):
        self.executed = []

    def execute(self, stmt):
        self.executed.append(stmt)


class FakeCtx:
    def __init__(self):
        self.conn = FakeConn()

    def __enter__(self):
        return self.conn

    def __exit__(self, *a):
        return False


class FakeEngine:
    def __init__(self):
        self.ctx = FakeCtx()

    def begin(self):
        return self.ctx


class SelectiveFailingEngine(FakeEngine):
    def __init__(self, fail_inserts=0):
        super().__init__()
        self.ctx = SelectiveFailingCtx(fail_inserts)


class SelectiveFailingCtx(FakeCtx):
    def __init__(self, fail_inserts):
        super().__init__()
        self.fail_inserts = fail_inserts
        self.sample_inserts = 0

    def __enter__(self):
        return SelectiveFailingConn(self)


class SelectiveFailingConn(FakeConn):
    def __init__(self, ctx):
        super().__init__()
        self._ctx = ctx

    def execute(self, stmt):
        table = getattr(stmt, "table", None)
        if table is not None and getattr(table, "name", "") == "latency_samples":
            self._ctx.sample_inserts += 1
            if self._ctx.sample_inserts <= self._ctx.fail_inserts:
                raise RuntimeError("db down")
        self.executed.append(stmt)


class AlwaysFailingCtx(FakeCtx):
    def __enter__(self):
        raise RuntimeError("db down always")


class AlwaysFailingEngine(FakeEngine):
    def __init__(self):
        self.ctx = AlwaysFailingCtx()


class QuiescentWriter(LatencySampleWriter):
    """测试用：写入线程不消费队列，便于确定性验证 stop() drain。"""

    def _run(self):
        self._stopped.wait()


def make_probe(stats=None, clock=None, on_sample=None, session=1, strategy=None):
    probe = WSProbe(
        insts=["BTC-USDT-SWAP"],
        channels=["trades"],
        ping_interval=1.0,
        ping_timeout=5.0,
        stats=stats or StatsRegistry(),
        summary_interval=60,
        clock=clock or FakeClock(),
        on_sample=on_sample or (lambda s: True),
        strategy=strategy,
    )
    probe.session = session
    probe._running = True
    return probe


def run_async(coro):
    return asyncio.run(coro)


# ----------------------------------------------------------------------
class TestPercentilesAndJitter(unittest.TestCase):
    def test_percentile_numpy_semantics(self):
        self.assertEqual(percentile([1], 50), 1.0)
        self.assertAlmostEqual(percentile([1, 2, 3, 4], 50), 2.5)
        self.assertAlmostEqual(percentile([1, 2, 3, 4], 25), 1.75)
        self.assertAlmostEqual(percentile([10, 20], 99), 19.9)

    def test_percentile_unsorted_input_sorts_first(self):
        """numpy 语义：输入无序也必须先排序（否则 p50 > p95 的错误结果）。"""
        self.assertAlmostEqual(percentile([10, 1, 9, 2], 50), 5.5)
        unsorted_vals = [100.0, 5.0, 5.0, 5.0, 5.0]
        self.assertLessEqual(percentile(unsorted_vals, 50),
                             percentile(unsorted_vals, 95))
        # 时序到达（未排序）窗口值的 p50/p95 单调性（冒烟回归）
        chronological = [1.0, 100.0, 2.0, 90.0, 3.0, 80.0, 4.0, 70.0]
        self.assertLessEqual(percentile(chronological, 50),
                             percentile(chronological, 95))

    def test_jitter_n_lt_2_zero(self):
        self.assertEqual(sample_stddev([]), 0.0)
        self.assertEqual(sample_stddev([5.0]), 0.0)
        self.assertGreater(sample_stddev([1.0, 2.0, 3.0]), 0.0)

    def test_window_start_fixed_utc(self):
        ts = datetime(2026, 8, 17, 10, 0, 59, tzinfo=UTC)
        self.assertEqual(window_start_for(ts, 60), datetime(2026, 8, 17, 10, 0, tzinfo=UTC))
        ts = datetime(2026, 8, 17, 10, 1, 0, tzinfo=UTC)
        self.assertEqual(window_start_for(ts, 60), datetime(2026, 8, 17, 10, 1, tzinfo=UTC))

    def test_stats_registry_prune_bounds_windows(self):
        """窗口簿记裁剪后 totals 仍完整（评审发现 #5）。"""
        stats = StatsRegistry()
        w_old = datetime(2026, 1, 1, tzinfo=UTC)
        w_new = datetime(2026, 8, 17, tzinfo=UTC)
        stats.increment(w_old, STAT_MARKET_SAMPLES)
        stats.increment(w_new, STAT_MARKET_SAMPLES)
        self.assertEqual(stats.total(STAT_MARKET_SAMPLES), 2)
        stats.prune(w_new)
        self.assertEqual(stats.windows(), [w_new])
        self.assertEqual(stats.snapshot(w_old), {})
        self.assertEqual(stats.total(STAT_MARKET_SAMPLES), 2)   # 累计不受裁剪影响


# ----------------------------------------------------------------------
class TestCorrectedAndSummarizer(unittest.TestCase):
    def _summarizer(self):
        captured = []
        stats = StatsRegistry()
        summ = WindowSummarizer(60, stats, on_close=lambda sr, st: captured.extend(sr))
        return stats, summ, captured

    def _sample(self, value, offset):
        return {
            "sample_ts": utcnow(),
            "metric": RAW_METRIC,
            "channel": "trades",
            "inst_id": "BTC-USDT-SWAP",
            "value_ms": value,
            "clock_offset_ms": offset,
        }

    def test_corrected_sign(self):
        """C-P0-3：corrected = raw - offset（本地超前 +20 -> 30；本地落后 -20 -> 30）。"""
        _, summ, captured = self._summarizer()
        summ.add_sample(self._sample(50, 20))
        summ.add_sample(self._sample(10, -20))
        summ.flush_all()
        corrected = [r for r in captured if r["metric"] == CORRECTED_METRIC]
        self.assertEqual(len(corrected), 1)
        self.assertAlmostEqual(corrected[0]["p50_ms"], 30.0)
        self.assertAlmostEqual(corrected[0]["min_ms"], 30.0)
        self.assertAlmostEqual(corrected[0]["max_ms"], 30.0)

    def test_clock_drift_per_sample_offset(self):
        """A(50,+20)->30, B(45,+15)->30；窗口级校正实现必须失败。"""
        _, summ, captured = self._summarizer()
        summ.add_sample(self._sample(50, 20))
        summ.add_sample(self._sample(45, 15))
        summ.flush_all()
        corrected = [r for r in captured if r["metric"] == CORRECTED_METRIC]
        self.assertEqual(len(corrected), 1)
        row = corrected[0]
        self.assertEqual(row["n"], 2)
        self.assertAlmostEqual(row["min_ms"], 30.0)
        self.assertAlmostEqual(row["max_ms"], 30.0)
        self.assertAlmostEqual(row["mean_ms"], 30.0)

    def test_corrected_uses_own_offset_not_window_wide(self):
        """若用窗口统一 offset（如 min-RTT 的 +20），B 会变 25 而非 30。"""
        _, summ, captured = self._summarizer()
        summ.add_sample(self._sample(50, 20))
        summ.add_sample(self._sample(45, 15))
        summ.flush_all()
        corrected = [r for r in captured if r["metric"] == CORRECTED_METRIC][0]
        self.assertAlmostEqual(corrected["max_ms"], 30.0)

    def test_negative_counts_raw_and_corrected_separate(self):
        stats, summ, _ = self._summarizer()
        now = utcnow()
        w = window_start_for(now, 60)
        summ.add_sample({"sample_ts": now, "metric": RAW_METRIC, "channel": "trades",
                         "inst_id": "X", "value_ms": -5.0, "clock_offset_ms": None})
        summ.add_sample({"sample_ts": now, "metric": RAW_METRIC, "channel": "trades",
                         "inst_id": "X", "value_ms": 10.0, "clock_offset_ms": 20.0})
        summ.flush_all()
        self.assertEqual(stats.window_value(w, STAT_NEG_RAW), 1)
        self.assertEqual(stats.window_value(w, STAT_NEG_CORRECTED), 1)

    def test_summary_rows_include_corrected(self):
        stats, summ, captured = self._summarizer()
        summ.add_sample(self._sample(50, -20))
        summ.flush_all()
        metrics = {r["metric"] for r in captured}
        self.assertIn(RAW_METRIC, metrics)
        self.assertIn(CORRECTED_METRIC, metrics)

    def test_corrected_sign_regression(self):
        """C-P0-3 符号回归：corrected = raw - offset。
        错误实现（raw + offset）必须失败。"""
        _, summ, captured = self._summarizer()
        summ.add_sample(self._sample(50, 20))     # 本地超前 +20 -> corrected 30
        summ.add_sample(self._sample(10, -20))    # 本地落后 -20 -> corrected 30
        summ.add_sample(self._sample(45, 15))     # -> corrected 30
        summ.flush_all()
        corrected = [r for r in captured if r["metric"] == CORRECTED_METRIC]
        self.assertEqual(len(corrected), 1)
        self.assertEqual(corrected[0]["n"], 3)
        self.assertAlmostEqual(corrected[0]["min_ms"], 30.0)
        self.assertAlmostEqual(corrected[0]["max_ms"], 30.0)
        self.assertAlmostEqual(corrected[0]["mean_ms"], 30.0)
        self.assertAlmostEqual(corrected[0]["p50_ms"], 30.0)

    def test_negative_corrected_warning(self):
        """C-P0-4：corrected < -10ms 记 WARNING + 计数；-10~0 只计数不告警。"""
        stats, summ, _ = self._summarizer()
        now = utcnow()
        w = window_start_for(now, 60)
        with self.assertLogs("app.latency.metrics", level="WARNING") as cm:
            summ.add_sample({"sample_ts": now, "metric": RAW_METRIC,
                             "channel": "trades", "inst_id": "X",
                             "value_ms": 10.0, "clock_offset_ms": 25.0})
        self.assertEqual(stats.window_value(w, STAT_NEG_CORRECTED), 1)
        self.assertTrue(any("Negative corrected latency" in m for m in cm.output))
        with self.assertNoLogs("app.latency.metrics", level="WARNING"):
            summ.add_sample({"sample_ts": now, "metric": RAW_METRIC,
                             "channel": "trades", "inst_id": "X",
                             "value_ms": 10.0, "clock_offset_ms": 15.0})
        self.assertEqual(stats.window_value(w, STAT_NEG_CORRECTED), 2)

    def test_negative_corrected_warning_rate_limited_per_window(self):
        """C-P0-4：每窗口至多一条告警。"""
        stats, summ, _ = self._summarizer()
        now = utcnow()
        w = window_start_for(now, 60)
        with self.assertLogs("app.latency.metrics", level="WARNING") as cm:
            summ.add_sample({"sample_ts": now, "metric": RAW_METRIC,
                             "channel": "trades", "inst_id": "X",
                             "value_ms": 10.0, "clock_offset_ms": 25.0})
            summ.add_sample({"sample_ts": now, "metric": RAW_METRIC,
                             "channel": "trades", "inst_id": "X",
                             "value_ms": 10.0, "clock_offset_ms": 30.0})
        warns = [m for m in cm.output if "Negative corrected latency" in m]
        self.assertEqual(len(warns), 1)
        self.assertEqual(stats.window_value(w, STAT_NEG_CORRECTED), 2)

    def test_corrected_never_persisted(self):
        row = dict(mk_market_sample())
        row["metric"] = CORRECTED_METRIC
        with self.assertRaises(ValueError):
            LatencySampleWriter._to_row(row)
        self.assertFalse(LatencySampleWriter._fields_valid(row))


# ----------------------------------------------------------------------
class TestChannelDetectParse(unittest.TestCase):
    def test_bbo_tbt_detection(self):
        self.assertTrue(detect_market_sample("bbo-tbt", {"bids": [], "asks": []}))
        self.assertTrue(detect_market_sample(
            "bbo-tbt", {"bids": [["1", "2"]], "asks": [["3", "4"]]}))
        self.assertFalse(detect_market_sample(
            "bbo-tbt", {"bestBid": "1", "bestAsk": "2"}))

    def test_all_channel_detection(self):
        self.assertTrue(detect_market_sample("trades", {"px": "100", "sz": "1"}))
        self.assertFalse(detect_market_sample("trades", {"px": "100"}))
        for ch in ("books5", "books", "books-l2-tbt", "books50-l2-tbt"):
            self.assertTrue(detect_market_sample(ch, {"bids": [], "asks": []}))
        self.assertTrue(detect_market_sample("mark-price", {"markPx": "1"}))
        self.assertTrue(detect_market_sample("index-tickers", {"idxPx": "1"}))
        self.assertTrue(detect_market_sample("tickers", {"last": "1"}))
        self.assertFalse(detect_market_sample("trades", "not-a-dict"))
        self.assertFalse(detect_market_sample("trades", {}))

    def test_parse_minimum(self):
        parsed, err = parse_market_sample(
            "trades", {"px": "1", "sz": "1", "instId": "BTC-USDT-SWAP", "ts": now_ms()})
        self.assertIsNotNone(parsed)
        self.assertIsNone(err)
        # 缺 tradeId/side 等不产生 parse_errors
        parsed, err = parse_market_sample(
            "trades", {"px": "1", "sz": "1", "ts": now_ms(), "instId": "X"})
        self.assertIsNotNone(parsed)

    def test_parse_errors_only_ts_instId(self):
        # ts 缺失
        parsed, err = parse_market_sample("trades", {"px": "1", "sz": "1", "instId": "X"})
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)
        # ts 非法
        parsed, err = parse_market_sample(
            "trades", {"px": "1", "sz": "1", "instId": "X", "ts": "abc"})
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)
        # instId 缺失
        parsed, err = parse_market_sample(
            "trades", {"px": "1", "sz": "1", "ts": now_ms()})
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)

    def test_book_family_uses_arg_instId_fallback(self):
        """实测（P1-24）：bbo-tbt/books* data 项内无 instId，须取 arg.instId。"""
        for ch in ("bbo-tbt", "books5", "books", "books-l2-tbt", "books50-l2-tbt"):
            parsed, err = parse_market_sample(
                ch, {"bids": [], "asks": [], "ts": now_ms()},
                fallback_inst="BTC-USDT-SWAP")
            self.assertIsNotNone(parsed, ch)
            self.assertEqual(parsed.inst_id, "BTC-USDT-SWAP", ch)
            # 无 fallback -> instId 失败 -> parse_errors
            parsed, err = parse_market_sample(
                ch, {"bids": [], "asks": [], "ts": now_ms()})
            self.assertIsNone(parsed, ch)
            self.assertIsNotNone(err, ch)


# ----------------------------------------------------------------------
class TestSequenceRules(unittest.TestCase):
    def _probe(self):
        stats = StatsRegistry()
        probe = make_probe(stats=stats)
        return probe, stats

    def _ps(self, channel="books", inst="BTC-USDT-SWAP"):
        return ParsedSample(channel, inst, 1)

    def test_snapshot_never_counts_gap_nor_reset(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(),
                         {"seqId": "100"}, w)
        probe._check_seq("snapshot", "books", self._ps(),
                         {"seqId": "10"}, w)   # snapshot 回退不是 reset
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 0)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 0)

    def test_update_gap_first(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(), {"seqId": "100"}, w)
        probe._check_seq("update", "books", self._ps(),
                         {"prevSeqId": "90", "seqId": "101"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 1)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 0)

    def test_reset_only_contiguous_rollback(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(), {"seqId": "100"}, w)
        probe._check_seq("update", "books", self._ps(),
                         {"prevSeqId": "100", "seqId": "99"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 0)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 1)

    def test_gap_precedes_reset_ordering(self):
        """prev != last 且 seq < prev：gap 优先，禁止 reset 优先。"""
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(), {"seqId": "100"}, w)
        probe._check_seq("update", "books", self._ps(),
                         {"prevSeqId": "50", "seqId": "1"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 1)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 0)

    def test_first_update_without_snapshot_no_count(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("update", "books50-l2-tbt",
                         ParsedSample("books50-l2-tbt", "ETH-USDT-SWAP", 1),
                         {"prevSeqId": "5", "seqId": "6"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 0)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 0)

    def test_no_action_no_count(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq(None, "books", self._ps(), {"seqId": "999"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 0)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 0)

    def test_seq_isolation_by_key(self):
        probe, stats = self._probe()
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(), {"seqId": "100"}, w)
        probe._check_seq("update", "books",
                         ParsedSample("books", "ETH-USDT-SWAP", 1),
                         {"prevSeqId": "999", "seqId": "1000"}, w)
        # 不同 inst_id 无 prior state -> 不计
        self.assertEqual(stats.window_value(w, STAT_SEQ_GAP), 0)
        probe._check_seq("update", "books", self._ps(),
                         {"prevSeqId": "100", "seqId": "98"}, w)
        self.assertEqual(stats.window_value(w, STAT_SEQ_RESET), 1)

    def test_reconnect_clears_seq_keeps_clock_offset(self):
        clock = FakeClock(offset=-18.5)
        stats = StatsRegistry()
        probe = make_probe(stats=stats, clock=clock)
        w = utcnow()
        probe._check_seq("snapshot", "books", self._ps(), {"seqId": "100"}, w)
        self.assertIn((1, "books", "BTC-USDT-SWAP"), probe.seq_states)
        probe._reset_for_reconnect()
        self.assertEqual(probe.seq_states, {})
        self.assertAlmostEqual(clock.active_offset(), -18.5)


# ----------------------------------------------------------------------
class TestPingProbe(unittest.TestCase):
    def test_ping_pairing(self):
        ping = PingProbe(1.0, 5.0)
        ws = FakeWS()
        run_async(ping.send(ws))
        self.assertIsNotNone(ping.pending)
        rtt = ping.on_pong()
        self.assertIsNotNone(rtt)
        self.assertIsNone(ping.pending)
        self.assertIsNone(ping.on_pong())  # stray pong

    def test_ping_timeout(self):
        ping = PingProbe(1.0, 5.0)
        ws = FakeWS()
        run_async(ping.send(ws))
        real = ws_probe_mod.time.monotonic_ns
        try:
            ws_probe_mod.time.monotonic_ns = lambda: real() + int(6e9)
            self.assertTrue(ping.check_timeout())
        finally:
            ws_probe_mod.time.monotonic_ns = real
        self.assertIsNone(ping.pending)

    def test_ping_probe_uses_app_layer_text(self):
        ping = PingProbe(1.0, 5.0)
        ws = FakeWS()
        run_async(ping.send(ws))
        self.assertEqual(ws.sent, ["ping"])


# ----------------------------------------------------------------------
class TestWSMessagesReceivedBoundary(unittest.TestCase):
    def test_message_counts(self):
        probe = make_probe()
        market = json.dumps({
            "arg": {"channel": "trades"},
            "data": [{"px": "1", "sz": "1", "ts": now_ms(), "instId": "X"}],
        })
        probe._ws = FakeWS([market])
        run_async(probe._recv_loop())
        w = probe._now_window()
        self.assertEqual(probe.stats.window_value(w, STAT_WS_MESSAGES), 1)

    def test_connection_closed_not_counted(self):
        probe = make_probe()
        probe._ws = FakeWS([])
        run_async(probe._recv_loop())
        w = probe._now_window()
        self.assertEqual(probe.stats.window_value(w, STAT_WS_MESSAGES), 0)
        self.assertEqual(probe._exit_reason, "socket_closed")

    def test_timeout_not_counted(self):
        probe = make_probe()
        probe._ws = FakeWSHang()

        async def _run():
            task = asyncio.create_task(probe._recv_loop())
            await asyncio.sleep(1.2)      # 至少触发一次 wait_for 超时
            probe._stop.set()
            await task
        run_async(_run())
        w = probe._now_window()
        self.assertEqual(probe.stats.window_value(w, STAT_WS_MESSAGES), 0)

    def test_json_string_market_dispatch(self):
        """websockets 17 文本帧均为 str：JSON 行情须在 str 分支内解析。"""
        probe = make_probe()
        market = json.dumps({
            "arg": {"channel": "trades"},
            "data": [{"px": "1", "sz": "1", "ts": now_ms(), "instId": "X"}],
        })
        probe._ws = FakeWS([market])
        run_async(probe._recv_loop())
        w = probe._now_window()
        self.assertEqual(probe.stats.window_value(w, STAT_WS_MESSAGES), 1)
        self.assertEqual(probe.stats.window_value(w, STAT_MARKET_SAMPLES), 1)
        self.assertEqual(probe.stats.window_value(w, STAT_SAMPLES_PARSED), 1)
        self.assertEqual(probe.stats.window_value(w, STAT_QUEUED), 1)

    def test_subscribe_ack_str_parsed(self):
        probe = make_probe()
        ack = json.dumps({
            "event": "subscribe",
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "connId": "abc", "code": None,
        })
        probe._ws = FakeWS([ack])
        probe._pending_subs = {("trades", "BTC-USDT-SWAP")}
        probe._active_subs = {("trades", "BTC-USDT-SWAP")}
        run_async(probe._recv_loop())
        self.assertTrue(probe._subscribe_done.is_set())
        self.assertIn(("trades", "BTC-USDT-SWAP"), probe._active_subs)


# ----------------------------------------------------------------------
class TestIdentityFields(unittest.TestCase):
    def test_http_rtt_identity(self):
        stats = StatsRegistry()
        captured = []
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: (5.0, 18.5, time.time_ns()),
                                   on_sample=captured.append)
        clock._run_probe_blocking()
        self.assertEqual(len(captured), 1)
        s = captured[0]
        self.assertEqual(s["session"], 0)
        self.assertEqual(s["inst_id"], SYSTEM_INST_ID)
        self.assertEqual(s["channel"], HTTP_CHANNEL)
        self.assertEqual(s["metric"], HTTP_RTT_METRIC)
        self.assertEqual(s["sample_ts"], s["recv_ts"])
        self.assertAlmostEqual(s["value_ms"], 5.0)
        self.assertAlmostEqual(s["clock_offset_ms"], 18.5)

    def test_ws_ping_identity(self):
        captured = []
        probe = make_probe(on_sample=captured.append)
        probe.session = 3
        run_async(probe.ping_probe.send(FakeWS()))
        probe._handle_pong(time.time_ns(), time.monotonic_ns())
        self.assertEqual(len(captured), 1)
        s = captured[0]
        self.assertEqual(s["session"], 3)
        self.assertEqual(s["inst_id"], SYSTEM_INST_ID)
        self.assertEqual(s["channel"], WS_CHANNEL)
        self.assertEqual(s["metric"], WS_PING_METRIC)

    def test_market_identity_and_sample_ts(self):
        captured = []
        probe = make_probe(on_sample=captured.append)
        probe.session = 7
        recv_ns = time.time_ns()
        probe._handle_market_message(
            {"arg": {"channel": "trades"},
             "data": [{"px": "1", "sz": "1", "ts": now_ms(),
                       "instId": "BTC-USDT-SWAP"}]},
            recv_ns, time.monotonic_ns())
        self.assertEqual(len(captured), 1)
        s = captured[0]
        self.assertEqual(s["session"], 7)
        self.assertEqual(s["channel"], "trades")
        self.assertEqual(s["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(s["metric"], RAW_METRIC)
        self.assertEqual(s["sample_ts"], s["recv_ts"])


# ----------------------------------------------------------------------
class TestUrlFallback(unittest.TestCase):
    def _connect_fn(self, acks_by_url):
        class FakeConnWS:
            def __init__(self, acks):
                self._msgs = [json.dumps(a) for a in acks]
                self.sent = []

            async def recv(self):
                if self._msgs:
                    return self._msgs.pop(0)
                raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)

            async def send(self, data):
                self.sent.append(data)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        async def connect_fn(url, **kwargs):
            if url in acks_by_url:
                return FakeConnWS(acks_by_url[url])
            raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)
        return connect_fn

    def test_fallback_after_cached_endpoint_fails(self):
        """缓存的工作端点随后不可达时，应回退到另一端点（评审发现）。"""
        stats = StatsRegistry()
        alt = ws_probe_mod.WS_URLS[1]
        cached = ws_probe_mod.WS_URLS[0]
        acks = [{"event": "subscribe",
                 "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}}]
        probe = WSProbe(
            insts=["BTC-USDT-SWAP"], channels=["trades"],
            ping_interval=1.0, ping_timeout=3.0, stats=stats,
            summary_interval=60, clock=FakeClock(), on_sample=lambda s: True,
            ws_connect_fn=self._connect_fn({alt: acks}),
        )
        # 模拟：cached 端点已工作过，随后不可达
        probe._working_url = cached
        probe._running = True
        reason = run_async(probe._connect_once())
        self.assertEqual(reason, "socket_closed")
        self.assertEqual(probe._working_url, alt)


# ----------------------------------------------------------------------
class TestMarketScopedCounters(unittest.TestCase):
    def test_probe_queued_dropped_market_only(self):
        stats = StatsRegistry()
        probe = make_probe(stats=stats, on_sample=lambda s: False)  # 队列满
        w = window_start_for(utcnow(), 60)
        probe._emit_sample(mk_market_sample())
        probe._emit_sample(mk_ping_sample())
        self.assertEqual(stats.window_value(w, STAT_QUEUED), 0)
        self.assertEqual(stats.window_value(w, STAT_DROPPED), 1)

    def test_probe_queued_on_success(self):
        stats = StatsRegistry()
        probe = make_probe(stats=stats, on_sample=lambda s: True)
        w = window_start_for(utcnow(), 60)
        probe._emit_sample(mk_market_sample())
        probe._emit_sample(mk_ping_sample())
        self.assertEqual(stats.window_value(w, STAT_QUEUED), 1)
        self.assertEqual(stats.window_value(w, STAT_DROPPED), 0)

    def test_market_written_by_window_split(self):
        w1 = utcnow().replace(minute=0, second=0, microsecond=0)
        w2 = w1.replace(minute=1)
        batch = [
            mk_market_sample(ts=w1),
            mk_market_sample(ts=w1),
            mk_market_sample(ts=w2),
            mk_ping_sample(ts=w1),
        ]
        out = market_written_by_window(batch, 60)
        self.assertEqual(out.get(w1), 2)
        self.assertEqual(out.get(w2), 1)
        self.assertEqual(sum(out.values()), 3)

    def test_writer_written_only_market(self):
        stats = StatsRegistry()
        engine = FakeEngine()
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            w = window_start_for(utcnow(), 60)
            writer._flush([mk_market_sample(), mk_ping_sample(),
                           dict(mk_market_sample())])
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 2)
        finally:
            writer.stop()

    def test_bbo_tbt_message_dispatch_uses_arg_inst(self):
        """bbo-tbt 消息（项内无 instId）经 arg.instId 解析成功。"""
        stats = StatsRegistry()
        captured = []
        probe = make_probe(stats=stats, on_sample=lambda s: (captured.append(s), True)[1])
        bbo = json.dumps({
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{"bids": [["1", "2"]], "asks": [["3", "4"]], "ts": now_ms()}],
        })
        probe._ws = FakeWS([bbo])
        run_async(probe._recv_loop())
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(captured[0]["channel"], "bbo-tbt")
        w = probe._now_window()
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 1)
        self.assertEqual(stats.window_value(w, STAT_SAMPLES_PARSED), 1)
        self.assertEqual(stats.window_value(w, STAT_QUEUED), 1)

    def test_field_length_violation_counts_parse_errors(self):
        """字段长度超限按 parse_errors 计，保持恒等式（评审发现）。"""
        stats = StatsRegistry()
        probe = make_probe(stats=stats, on_sample=lambda s: True)
        long_inst = "X" * 51
        probe._handle_market_message(
            {"arg": {"channel": "trades"},
             "data": [{"px": "1", "sz": "1", "ts": now_ms(), "instId": long_inst}]},
            time.time_ns(), time.monotonic_ns())
        w = probe._now_window()
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 1)
        self.assertEqual(stats.window_value(w, STAT_PARSE_ERRORS), 1)
        self.assertEqual(stats.window_value(w, STAT_SAMPLES_PARSED), 0)
        parsed = stats.window_value(w, STAT_SAMPLES_PARSED)
        self.assertEqual(parsed,
                         stats.window_value(w, STAT_MARKET_SAMPLES)
                         - stats.window_value(w, STAT_PARSE_ERRORS))

    def test_identity_holds_parsed_queued(self):
        stats = StatsRegistry()
        probe = make_probe(stats=stats, on_sample=lambda s: True)
        w = window_start_for(utcnow(), 60)
        probe._handle_market_message(
            {"arg": {"channel": "trades"},
             "data": [{"px": "1", "sz": "1", "ts": now_ms(), "instId": "X"}]},
            time.time_ns(), time.monotonic_ns())
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 1)
        self.assertEqual(stats.window_value(w, STAT_SAMPLES_PARSED), 1)
        self.assertEqual(stats.window_value(w, STAT_QUEUED), 1)
        # 非检测项不计
        probe._handle_market_message(
            {"arg": {"channel": "trades"},
             "data": [{"px": "1"}]},
            time.time_ns(), time.monotonic_ns())
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 1)
        # ts 缺失 -> parse_errors
        probe._handle_market_message(
            {"arg": {"channel": "trades"},
             "data": [{"px": "1", "sz": "1", "instId": "X"}]},
            time.time_ns(), time.monotonic_ns())
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 2)
        self.assertEqual(stats.window_value(w, STAT_PARSE_ERRORS), 1)
        self.assertEqual(stats.window_value(w, STAT_SAMPLES_PARSED), 1)


# ----------------------------------------------------------------------
class TestWriterFlushStateMachine(unittest.TestCase):
    def test_flush_fail_write_errors_retry_success_written(self):
        stats = StatsRegistry()
        engine = SelectiveFailingEngine(fail_inserts=2)
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            w = window_start_for(utcnow(), 60)
            sample = mk_market_sample()
            for _ in range(2):
                with self.assertRaises(RuntimeError):
                    writer._flush([sample])
            writer._flush([sample])          # 第三次成功
            self.assertEqual(stats.window_value(w, STAT_WRITE_ERRORS), 2)
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 1)
        finally:
            writer.stop()

    def test_dropped_never_from_db_failure(self):
        stats = StatsRegistry()
        engine = SelectiveFailingEngine(fail_inserts=99)
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            w = window_start_for(utcnow(), 60)
            with self.assertRaises(RuntimeError):
                writer._flush([mk_market_sample()])
            self.assertEqual(stats.window_value(w, STAT_DROPPED), 0)
            self.assertEqual(stats.window_value(w, STAT_WRITE_ERRORS), 1)
        finally:
            writer.stop()

    def test_written_eventually_consistent(self):
        """已关闭窗口仍可收到迟到 written 更新（P1-21），writer 直接 UPSERT。"""
        stats = StatsRegistry()
        captured = []
        summ = WindowSummarizer(60, stats, on_close=lambda sr, st: captured.extend(sr))
        engine = FakeEngine()
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            now = utcnow()
            # 先让窗口关闭
            summ.add_sample(mk_market_sample(ts=now, channel="trades"))
            summ.flush_all()
            w = window_start_for(now, 60)
            self.assertTrue(summ.is_window_closed(w))
            # 迟到的 commit：writer 直接写该窗口 written（累计值）
            writer._flush([mk_market_sample(ts=now)])
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 1)
            self.assertEqual(writer._committed_written.get(w), 1)
            # 再迟到一条：累计为 2，且引擎收到 value=2 的 written UPSERT
            writer._flush([mk_market_sample(ts=now)])
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 2)
            self.assertEqual(writer._committed_written.get(w), 2)
            stats_stmt = None
            for stmt in engine.ctx.conn.executed:
                table = getattr(stmt, "table", None)
                if table is not None and getattr(table, "name", "") == "latency_probe_stats":
                    stats_stmt = stmt
            self.assertIsNotNone(stats_stmt)
        finally:
            writer.stop()

    def test_written_retry_no_double_count(self):
        """失败重试不得重复计数 written（P1-21 / 评审发现）。"""
        stats = StatsRegistry()
        engine = SelectiveFailingEngine(fail_inserts=1)
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            w = window_start_for(utcnow(), 60)
            sample = mk_market_sample()
            with self.assertRaises(RuntimeError):
                writer._flush([sample])
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 0)   # 回滚
            self.assertEqual(writer._committed_written.get(w, 0), 0)
            writer._flush([sample])                                    # 重试成功
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 1)
            self.assertEqual(writer._committed_written.get(w), 1)
            writer._flush([sample])                                    # 第二批同窗口
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 2)
            self.assertEqual(writer._committed_written.get(w), 2)
        finally:
            writer.stop()

    def test_stop_always_failing_all_final_failed(self):
        """C-P0-5(a)：引擎永远失败 + N 条入队 -> stop() 后 written=0、
        final_failed=N、无缺口。"""
        stats = StatsRegistry()
        writer = LatencySampleWriter(stats, 60, engine=AlwaysFailingEngine())
        try:
            w = window_start_for(utcnow(), 60)
            n = 5
            for _ in range(n):
                self.assertTrue(writer.put(mk_market_sample()))
            stats.add(w, {STAT_QUEUED: n})
            writer.stop(timeout=3.0)
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 0)
            self.assertEqual(stats.window_value(w, STAT_FINAL_FAILED), n)
            self.assertEqual(stats.window_value(w, STAT_QUEUED),
                             stats.window_value(w, STAT_WRITTEN)
                             + stats.window_value(w, STAT_FINAL_FAILED))
        finally:
            writer.stop()

    def test_stop_mixed_success_failure_reconciles(self):
        """C-P0-5(b)：混合成功/失败批次 -> written + final_failed == queued。"""
        stats = StatsRegistry()
        engine = SelectiveFailingEngine(fail_inserts=2)
        writer = LatencySampleWriter(stats, 60, engine=engine)
        try:
            w = window_start_for(utcnow(), 60)
            n = 5
            for _ in range(n):
                self.assertTrue(writer.put(mk_market_sample()))
            stats.add(w, {STAT_QUEUED: n})
            writer.stop(timeout=5.0)
            written = stats.window_value(w, STAT_WRITTEN)
            final_failed = stats.window_value(w, STAT_FINAL_FAILED)
            self.assertEqual(written + final_failed, n)   # 无缺口
            self.assertEqual(stats.window_value(w, STAT_QUEUED), written + final_failed)
        finally:
            writer.stop()

    def test_pending_rows_drained_by_stop(self):
        """C-P0-5(c)：stop() 必须 drain `_pending`（重试中批次）剩余行。"""
        stats = StatsRegistry()
        writer = QuiescentWriter(stats, 60, engine=AlwaysFailingEngine())
        try:
            w = window_start_for(utcnow(), 60)
            writer._pending = [mk_market_sample(), mk_market_sample()]
            writer.stop()
            self.assertEqual(stats.window_value(w, STAT_WRITTEN), 0)
            self.assertEqual(stats.window_value(w, STAT_FINAL_FAILED), 2)
            self.assertEqual(writer._pending, [])
        finally:
            writer.stop()


# ----------------------------------------------------------------------
class TestClockProbe(unittest.TestCase):
    def test_http_probe_failure(self):
        stats = StatsRegistry()
        captured = []
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None,
                                   on_sample=captured.append)
        clock._record_success(10.0, -20.0)
        self.assertAlmostEqual(clock.active_offset(), -20.0)
        clock._run_probe_blocking()
        w = window_start_for(utcnow(), 60)
        self.assertEqual(stats.window_value(w, STAT_HTTP_ERRORS), 1)
        self.assertEqual(len(captured), 0)          # 不产生 http_rtt sample
        self.assertAlmostEqual(clock.active_offset(), -20.0)  # 保留上次有效 offset

    def test_failure_not_in_calibration_window(self):
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None)
        clock._record_success(5.0, -10.0)
        clock._run_probe_blocking()                 # 失败
        self.assertEqual(len(clock._probes), 1)     # 不入窗

    def test_active_offset_is_min_rtt(self):
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None)
        clock._record_success(30.0, -50.0)
        clock._record_success(10.0, -18.5)
        clock._record_success(20.0, -30.0)
        self.assertAlmostEqual(clock.active_offset(), -18.5)

    def test_calibration_window_keeps_latest_20(self):
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None, window_size=20)
        for i in range(25):
            clock._record_success(float(i), float(i))
        self.assertEqual(len(clock._probes), 20)

    def test_startup_probes_sequential(self):
        stats = StatsRegistry()
        concurrency = {"cur": 0, "max": 0}
        lock = threading.Lock()

        def slow_probe():
            with lock:
                concurrency["cur"] += 1
                concurrency["max"] = max(concurrency["max"], concurrency["cur"])
            time.sleep(0.01)
            with lock:
                concurrency["cur"] -= 1
            return (1.0, 0.0, time.time_ns())

        clock = ClockOffsetTracker(stats, 60, probe_fn=slow_probe)
        clock.startup_calibrate(5)
        self.assertEqual(concurrency["max"], 1)

    def test_no_valid_calibration_active_offset_none(self):
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None)
        self.assertIsNone(clock.active_offset())
        clock._run_probe_blocking()
        self.assertIsNone(clock.active_offset())

    def test_probe_once_rtt_uses_monotonic(self):
        """C-P0-1：RTT 用 monotonic delta；offset 用 wall midpoint。"""
        import app.latency.clock as clock_mod
        real_mono = clock_mod.time.monotonic_ns
        real_wall = clock_mod.time.time_ns
        real_get = clock_mod.requests.get

        class FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"data": [{"ts": "2050"}]}

        try:
            clock_mod.requests.get = lambda *a, **kw: FakeResp()
            # monotonic 序列：t0=1e15 ns, t1=1e15+1e6 ns -> RTT 恰为 1.0ms
            clock_mod.time.monotonic_ns = iter([10 ** 15, 10 ** 15 + 10 ** 6]).__next__
            # wall 序列：t0=2e15 ns, t1=2e15+4e6 ns -> 若错误用 wall 算 RTT 会得 4.0ms
            clock_mod.time.time_ns = iter([2 * 10 ** 15, 2 * 10 ** 15 + 4 * 10 ** 6]).__next__
            result = clock_mod.probe_once()
            self.assertIsNotNone(result)
            rtt_ms, offset_ms, t1_ns = result
            self.assertAlmostEqual(rtt_ms, 1.0)          # monotonic delta -> 1.0ms
            self.assertEqual(t1_ns, 2 * 10 ** 15 + 4 * 10 ** 6)
            midpoint_ns = (2 * 10 ** 15 + 2 * 10 ** 15 + 4 * 10 ** 6) // 2
            expected_offset = (midpoint_ns - 2050 * 1e6) / 1e6
            self.assertAlmostEqual(offset_ms, expected_offset)
        finally:
            clock_mod.requests.get = real_get
            clock_mod.time.monotonic_ns = real_mono
            clock_mod.time.time_ns = real_wall

    def test_offset_jump_detection(self):
        """C-P0-2：active offset 跳变 > 50ms 告警且不拒绝新值。"""
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None)
        clock._record_success(5.0, 10.0)      # active = 10.0
        clock._record_success(3.0, 20.0)      # 新 min-RTT -> active = 20.0，delta 10 < 50
        self.assertEqual(clock._offset_jumps, 0)
        self.assertAlmostEqual(clock.active_offset(), 20.0)
        with self.assertLogs("app.latency.clock", level="WARNING") as cm:
            clock._record_success(2.0, 80.0)  # 新 min-RTT -> active = 80.0，delta 60 > 50
        self.assertEqual(clock._offset_jumps, 1)
        self.assertAlmostEqual(clock.active_offset(), 80.0)
        self.assertTrue(any("CLOCK_OFFSET_JUMP" in m for m in cm.output))

    def test_probe_record_keeps_t1_and_active_probe(self):
        """C-P1-1：probe 记录含 t1_ns；active_probe() 返回所选 min-RTT 探测。"""
        stats = StatsRegistry()
        clock = ClockOffsetTracker(stats, 60, probe_fn=lambda: None)
        clock._record_success(30.0, -50.0, 111)
        clock._record_success(10.0, -18.5, 222)
        clock._record_success(20.0, -30.0, 333)
        self.assertAlmostEqual(clock.active_offset(), -18.5)
        active = clock.active_probe()
        self.assertIsNotNone(active)
        rtt, offset, t1 = active
        self.assertAlmostEqual(rtt, 10.0)
        self.assertAlmostEqual(offset, -18.5)
        self.assertEqual(t1, 222)
        # 窗口裁剪后记录仍保持 t1_ns（C-P1-1）
        clock2 = ClockOffsetTracker(stats, 60, probe_fn=lambda: None, window_size=2)
        for i in range(4):
            clock2._record_success(float(i), float(i * 2), i * 1000)
        self.assertEqual(len(clock2._probes), 2)
        self.assertAlmostEqual(clock2._probes[0][2], 2000)


# ----------------------------------------------------------------------
class TestMockStrategy(unittest.TestCase):
    def test_strategy_tolerance_heavy(self):
        stats = StatsRegistry()
        emitted = []
        strat = MockStrategy("heavy", stats, 60, on_sample=emitted.append,
                             queue_capacity=100)
        sample = dict(mk_market_sample())
        sample["arrival_mono_ns"] = time.monotonic_ns()
        for _ in range(10):
            strat.submit(sample)
        time.sleep(0.6)
        strat.stop()
        triples = [emitted[i:i + 3] for i in range(0, len(emitted), 3)]
        self.assertGreaterEqual(len(triples), 1)
        for feature, model, strat_ in triples:
            self.assertEqual(feature["metric"], STRATEGY_FEATURE_METRIC)
            self.assertEqual(model["metric"], STRATEGY_MODEL_METRIC)
            self.assertEqual(strat_["metric"], STRATEGY_METRIC)
            # P1-18：strategy + eps >= feature + model（非严格恒等式）
            self.assertGreaterEqual(
                strat_["value_ms"] + 5.0, feature["value_ms"] + model["value_ms"])

    def test_light_emits_strategy_latency(self):
        stats = StatsRegistry()
        emitted = []
        strat = MockStrategy("light", stats, 60, on_sample=emitted.append,
                             queue_capacity=100)
        for i in range(20):
            sample = dict(mk_market_sample())
            sample["arrival_mono_ns"] = time.monotonic_ns()
            sample["raw_item"] = {"px": str(100 + (i % 5) * 0.5), "sz": "1"}
            strat.submit(sample)
        time.sleep(0.4)
        strat.stop()
        self.assertGreaterEqual(len(emitted), 1)
        for s in emitted:
            self.assertEqual(s["metric"], STRATEGY_METRIC)
            self.assertEqual(s["channel"], "trades")
            self.assertEqual(s["inst_id"], "BTC-USDT-SWAP")
            self.assertEqual(s["session"], 1)

    def test_strategy_overflow_counts_dropped(self):
        stats = StatsRegistry()
        emitted = []
        strat = MockStrategy("heavy", stats, 60, on_sample=emitted.append,
                             queue_capacity=1, put_timeout=0.0)
        sample = dict(mk_market_sample())
        sample["arrival_mono_ns"] = time.monotonic_ns()
        for _ in range(50):
            strat.submit(sample)
        w = window_start_for(utcnow(), 60)
        self.assertGreater(stats.window_value(w, STAT_STRATEGY_DROPPED), 0)
        strat.stop()

    def test_strategy_no_double_write(self):
        """C-P0-6：heavy 策略下一条市场消息 -> RAW_METRIC 恰好 1 次；
        strategy_* 均为 non_market 且无 raw_item。"""
        stats = StatsRegistry()
        emitted = []
        strat = MockStrategy("heavy", stats, 60, on_sample=emitted.append,
                             queue_capacity=100)
        probe = make_probe(stats=stats, on_sample=emitted.append, strategy=strat)
        try:
            probe._handle_market_message(
                {"arg": {"channel": "trades"},
                 "data": [{"px": "1", "sz": "1", "ts": now_ms(),
                           "instId": "BTC-USDT-SWAP"}]},
                time.time_ns(), time.monotonic_ns())
            time.sleep(0.4)
            strat.stop()
            raw = [s for s in emitted if s["metric"] == RAW_METRIC]
            self.assertEqual(len(raw), 1)
            for s in emitted:
                if s["metric"] == RAW_METRIC:
                    self.assertEqual(s["sample_class"], SAMPLE_CLASS_MARKET)
                    continue
                self.assertTrue(s["metric"].startswith("strategy_"), s["metric"])
                self.assertEqual(s["sample_class"], SAMPLE_CLASS_NON_MARKET)
                self.assertNotIn("raw_item", s)
        finally:
            strat.stop()


# ----------------------------------------------------------------------
class TestEventLoopLag(unittest.TestCase):
    def test_lag_percentiles_empty(self):
        out = lag_percentiles([])
        self.assertEqual(out, {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0})

    def test_lag_percentiles_synthetic(self):
        """C-P1-2：合成 lag 序列 -> p50/p95/p99/max 正确。"""
        out = lag_percentiles([1, 2, 3, 4])
        self.assertAlmostEqual(out["p50"], 2.5)
        self.assertAlmostEqual(out["p95"], 3.85)
        self.assertAlmostEqual(out["p99"], 3.97)
        self.assertEqual(out["max"], 4.0)

    def test_lag_monitor_summary(self):
        from cli.latency_probe import EventLoopLagMonitor
        mon = EventLoopLagMonitor()
        mon._lags.extend([1.0, 2.0, 3.0])
        out = mon.summary()
        self.assertEqual(out["max"], 3.0)
        self.assertAlmostEqual(out["p50"], 2.0)
        mon.reset()
        self.assertEqual(mon.summary()["max"], 0.0)


# ----------------------------------------------------------------------
class TestWsEndpointStats(unittest.TestCase):
    def _failing_conn(self):
        class FakeConnWS:
            async def recv(self):
                raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)

            async def send(self, data):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False
        return FakeConnWS()

    def test_endpoint_connect_stats(self):
        """C-P1-3：注入 ws_connect_fn 统计 attempts/failures/fallbacks。"""
        stats = StatsRegistry()
        attempts = []

        async def connect_fn(url, **kwargs):
            attempts.append(url)
            if url == ws_probe_mod.WS_URLS[0]:
                raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)
            return self._failing_conn()

        probe = WSProbe(
            insts=["BTC-USDT-SWAP"], channels=["trades"],
            ping_interval=1.0, ping_timeout=3.0, stats=stats,
            summary_interval=60, clock=FakeClock(), on_sample=lambda s: True,
            ws_connect_fn=connect_fn,
        )
        probe._running = True
        reason = run_async(probe._connect_once())
        totals = stats.totals()
        self.assertEqual(reason, "socket_closed")
        self.assertEqual(probe._working_url, ws_probe_mod.WS_URLS[1])
        # 用 totals（累计）断言，避免 5s 订阅等待跨 UTC 分钟边界导致窗口归属漂移
        self.assertEqual(totals.get(STAT_WS_CONNECT_ATTEMPTS), 2)
        self.assertEqual(totals.get(STAT_WS_CONNECT_FAILURES), 1)
        self.assertEqual(totals.get(STAT_WS_ENDPOINT_FALLBACKS), 1)
        self.assertEqual(totals.get(STAT_WS_CONNECT), 1)   # session 级不变

    def test_single_url_all_fail_no_fallback_count(self):
        """单端点全部失败：attempts=failures=N，fallbacks=0（C-P1-3）。"""
        stats = StatsRegistry()
        orig_urls = ws_probe_mod.WS_URLS
        ws_probe_mod.WS_URLS = ["wss://only-endpoint"]

        async def connect_fn(url, **kwargs):
            raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)

        probe = WSProbe(
            insts=["BTC-USDT-SWAP"], channels=["trades"],
            ping_interval=1.0, ping_timeout=3.0, stats=stats,
            summary_interval=60, clock=FakeClock(), on_sample=lambda s: True,
            ws_connect_fn=connect_fn,
        )
        probe._running = True
        try:
            reason = run_async(probe._connect_once())
        finally:
            ws_probe_mod.WS_URLS = orig_urls
        w = probe._now_window()
        self.assertEqual(reason, "socket_closed")
        self.assertEqual(stats.window_value(w, STAT_WS_CONNECT_ATTEMPTS), 1)
        self.assertEqual(stats.window_value(w, STAT_WS_CONNECT_FAILURES), 1)
        self.assertEqual(stats.window_value(w, STAT_WS_ENDPOINT_FALLBACKS), 0)

    def test_shutdown_during_subscribe_completes_cleanly(self):
        """关闭信号在订阅等待期间到达 -> probe_task 正常结束、attempts 不欠计。"""
        stats = StatsRegistry()
        orig_timeout = ws_probe_mod.SUBSCRIBE_TIMEOUT
        ws_probe_mod.SUBSCRIBE_TIMEOUT = 0.2
        try:

            class LiveConn:
                def __init__(self):
                    self.sent = []

                async def recv(self):
                    await asyncio.sleep(3600)

                async def send(self, data):
                    self.sent.append(data)

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *a):
                    return False

            async def connect_fn(url, **kwargs):
                if url == ws_probe_mod.WS_URLS[0]:
                    raise websockets.exceptions.ConnectionClosed(rcvd=None, sent=None)
                return LiveConn()

            probe = WSProbe(
                insts=["BTC-USDT-SWAP"], channels=["trades"],
                ping_interval=1.0, ping_timeout=3.0, stats=stats,
                summary_interval=60, clock=FakeClock(), on_sample=lambda s: True,
                ws_connect_fn=connect_fn,
            )

            async def drive():
                task = asyncio.create_task(probe.run())
                await asyncio.sleep(0.15)      # 连接 + 订阅进行中
                probe.stop()
                await asyncio.wait_for(task, timeout=3.0)

            run_async(drive())
            w = probe._now_window()
            self.assertEqual(stats.window_value(w, STAT_WS_CONNECT_ATTEMPTS), 2)
            self.assertEqual(stats.window_value(w, STAT_WS_CONNECT_FAILURES), 1)
            self.assertEqual(stats.window_value(w, STAT_WS_ENDPOINT_FALLBACKS), 1)
        finally:
            ws_probe_mod.SUBSCRIBE_TIMEOUT = orig_timeout


# ----------------------------------------------------------------------
class TestReconnectBackoff(unittest.TestCase):
    def test_backoff_sequence(self):
        """C-P1-5：注入 jitter=0 断言退避序列 [1,2,4,8,16,30,30,...]。"""
        probe = make_probe()
        probe._jitter_fn = lambda lo, hi: lo
        for expected in [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]:
            self.assertAlmostEqual(probe._next_reconnect_delay(), expected)

    def test_backoff_reset_after_stable_connection(self):
        """C-P1-5：连接稳定运行 >=60s 后连续失败计数归零。"""
        probe = make_probe()
        probe._consecutive_failures = 3
        probe._conn_up_mono = 100.0
        real = ws_probe_mod.time.monotonic
        try:
            ws_probe_mod.time.monotonic = lambda: 100.0 + 61.0
            probe._maybe_reset_backoff()
            self.assertEqual(probe._consecutive_failures, 0)
            probe._consecutive_failures = 3
            probe._conn_up_mono = 100.0
            ws_probe_mod.time.monotonic = lambda: 100.0 + 10.0
            probe._maybe_reset_backoff()
            self.assertEqual(probe._consecutive_failures, 3)   # 未达 60s 不归零
        finally:
            ws_probe_mod.time.monotonic = real


# ----------------------------------------------------------------------
class TestWsMessageClassification(unittest.TestCase):
    def test_control_unknown_parse_error_counts(self):
        """C-P1-6：control / unknown / ws_parse_errors 分类计数。"""
        stats = StatsRegistry()
        probe = make_probe(stats=stats)
        w = probe._now_window()
        ack = json.dumps({"event": "subscribe",
                          "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
                          "code": None})
        probe._pending_subs = {("trades", "BTC-USDT-SWAP")}
        probe._active_subs = {("trades", "BTC-USDT-SWAP")}
        probe._ws = FakeWS([ack, "not-json{{", '{"foo": 1}', "ping", "pong"])
        run_async(probe._recv_loop())
        self.assertEqual(stats.window_value(w, STAT_WS_CONTROL), 3)     # ack+ping+pong
        self.assertEqual(stats.window_value(w, STAT_WS_PARSE_ERRORS), 1)
        self.assertEqual(stats.window_value(w, STAT_WS_UNKNOWN), 1)
        self.assertEqual(stats.window_value(w, STAT_WS_MESSAGES), 5)    # 应用层消息均 +1

    def test_market_message_not_classified_as_control(self):
        stats = StatsRegistry()
        probe = make_probe(stats=stats)
        w = probe._now_window()
        market = json.dumps({
            "arg": {"channel": "trades"},
            "data": [{"px": "1", "sz": "1", "ts": now_ms(), "instId": "X"}],
        })
        probe._ws = FakeWS([market])
        run_async(probe._recv_loop())
        self.assertEqual(stats.window_value(w, STAT_WS_MESSAGES), 1)
        self.assertEqual(stats.window_value(w, STAT_WS_CONTROL), 0)
        self.assertEqual(stats.window_value(w, STAT_WS_UNKNOWN), 0)
        self.assertEqual(stats.window_value(w, STAT_MARKET_SAMPLES), 1)


# ----------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()
