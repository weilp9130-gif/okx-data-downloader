"""Phase 10 测试：OrderBook 频道能力校验 / 周期采样 / DATA_CONFLICT / Retention 配置"""

import unittest
from datetime import datetime, timezone

from app.conflict import DataConflictDetector, canonical_hash
from app.realtime.orderbook import (
    ORDERBOOK_CHANNELS,
    OrderBookHandler,
    describe_channel,
    validate_channel,
)


class TestOrderBookChannelCapabilities(unittest.TestCase):
    def test_describe_known_channel(self):
        caps = describe_channel("books")
        self.assertEqual(caps["depth"], 400)
        self.assertTrue(caps["incremental"])
        self.assertEqual(caps["vip"], 0)

    def test_describe_unknown_channel_raises(self):
        with self.assertRaises(ValueError):
            describe_channel("books999")

    def test_validate_public_channel(self):
        caps = validate_channel("books5")
        self.assertEqual(caps["depth"], 5)

    def test_validate_vip_channel_rejected_by_default(self):
        with self.assertRaises(ValueError) as ctx:
            validate_channel("books50-l2-tbt")
        self.assertIn("VIP", str(ctx.exception))

    def test_validate_vip_channel_allowed_when_confirmed(self):
        caps = validate_channel("books50-l2-tbt", allow_vip=True)
        self.assertEqual(caps["depth"], 50)

    def test_all_channels_have_capability_fields(self):
        for name, caps in ORDERBOOK_CHANNELS.items():
            self.assertIn("depth", caps, name)
            self.assertIn("incremental", caps, name)
            self.assertIn("vip", caps, name)


class TestOrderBookHandlerDepth(unittest.TestCase):
    def test_depth_from_channel(self):
        self.assertEqual(OrderBookHandler("BTC-USDT-SWAP", channel="books5").depth, 5)
        self.assertEqual(OrderBookHandler("BTC-USDT-SWAP", channel="books").depth, 400)

    def test_snapshot_levels_truncation(self):
        handler = OrderBookHandler("BTC-USDT-SWAP", channel="books", snapshot_levels=2)
        handler.handle({
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "bids": [["100", "1"], ["99", "2"], ["98", "3"]],
                "asks": [["101", "1"], ["102", "2"], ["103", "3"]],
                "ts": "1786862438257",
                "seqId": "10",
                "prevSeqId": "9",
            }],
        })
        record = handler.periodic_snapshot()
        self.assertIsNotNone(record)
        self.assertEqual(len(record["bids"]), 2)
        self.assertEqual(len(record["asks"]), 2)
        self.assertEqual(record["snapshot_type"], "PERIODIC")
        # 完整盘口不受 snapshot_levels 截断
        bids, asks = handler.full_book()
        self.assertEqual(len(bids), 3)
        self.assertEqual(len(asks), 3)

    def test_periodic_snapshot_requires_running(self):
        handler = OrderBookHandler("BTC-USDT-SWAP")
        self.assertIsNone(handler.periodic_snapshot())

    def test_received_at_recorded(self):
        handler = OrderBookHandler("BTC-USDT-SWAP")
        handler.handle({
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "bids": [["100", "1"]],
                "asks": [["101", "1"]],
                "ts": "1786862438257",
                "seqId": "10",
                "prevSeqId": "9",
            }],
        })
        self.assertIsInstance(handler.state.last_received_at, datetime)
        record = handler.periodic_snapshot()
        self.assertIsNotNone(record["received_at"])


class TestCanonicalHash(unittest.TestCase):
    def test_key_order_independent(self):
        a = {"tradeId": "1", "px": "100", "sz": "1"}
        b = {"sz": "1", "px": "100", "tradeId": "1"}
        self.assertEqual(canonical_hash(a), canonical_hash(b))

    def test_value_change_alters_hash(self):
        a = {"tradeId": "1", "px": "100"}
        b = {"tradeId": "1", "px": "101"}
        self.assertNotEqual(canonical_hash(a), canonical_hash(b))


class TestDataConflictDetector(unittest.TestCase):
    INST = "CONFLICT-TEST-SWAP"

    def setUp(self):
        self.detector = DataConflictDetector()
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        from sqlalchemy import text
        with self.detector.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM trades WHERE inst_id = :i"), {"i": self.INST}
            )
            conn.execute(
                text("DELETE FROM data_conflicts WHERE inst_id = :i"), {"i": self.INST}
            )

    def _row(self, px: str):
        raw = {"instId": self.INST, "tradeId": "c-1", "px": px, "sz": "1",
               "side": "buy", "ts": "1786862438257"}
        return {
            "inst_id": self.INST,
            "trade_id": "c-1",
            "ts": datetime(2026, 8, 16, 12, 0, 38, 257000, tzinfo=timezone.utc),
            "px": px,
            "sz": "1",
            "side": "buy",
            "source": "REST",
            "ingested_at": datetime.now(timezone.utc),
            "raw_json": raw,
            "raw_hash": canonical_hash(raw),
        }

    def test_no_conflict_on_empty_table(self):
        safe, conflicts = self.detector.detect_trades([self._row("100")])
        self.assertEqual(len(safe), 1)
        self.assertEqual(conflicts, [])

    def test_same_hash_is_duplicate_not_conflict(self):
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from app.models import Trade

        row = self._row("100")
        with self.detector.engine.begin() as conn:
            conn.execute(pg_insert(Trade).values([row]).on_conflict_do_nothing())

        safe, conflicts = self.detector.detect_trades([self._row("100")])
        self.assertEqual(conflicts, [])
        self.assertEqual(len(safe), 1)

    def test_different_hash_registers_conflict(self):
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from app.models import Trade

        with self.detector.engine.begin() as conn:
            conn.execute(
                pg_insert(Trade).values([self._row("100")]).on_conflict_do_nothing()
            )

        safe, conflicts = self.detector.detect_trades([self._row("999")])
        self.assertEqual(safe, [])
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["table_name"], "trades")
        self.assertEqual(self.detector.open_count("trades", self.INST), 1)

    def test_conflict_registration_is_deduped(self):
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from app.models import Trade

        with self.detector.engine.begin() as conn:
            conn.execute(
                pg_insert(Trade).values([self._row("100")]).on_conflict_do_nothing()
            )

        self.detector.detect_trades([self._row("999")])
        self.detector.detect_trades([self._row("999")])
        self.assertEqual(self.detector.open_count("trades", self.INST), 1)


class TestRetentionConfig(unittest.TestCase):
    def test_defaults_disabled(self):
        from app.config import RetentionConfig
        cfg = RetentionConfig()
        self.assertFalse(cfg.enabled)
        self.assertEqual(cfg.order_book_snapshots_days, 30)

    def test_orderbook_config_defaults(self):
        from app.config import OrderBookConfig
        cfg = OrderBookConfig()
        self.assertEqual(cfg.channel, "books")
        self.assertEqual(cfg.snapshot_interval, 5)
        self.assertEqual(cfg.snapshot_levels, 5)
        self.assertFalse(cfg.allow_vip)


class TestPhase10Models(unittest.TestCase):
    def test_data_conflict_model(self):
        from app.models import DataConflict
        self.assertEqual(DataConflict.__tablename__, "data_conflicts")


if __name__ == "__main__":
    unittest.main()
