"""Trades 单元测试（离线，基于 fixture）"""

import json
import unittest

from tests.path_utils import FIXTURES

from app.downloader.trades import TradesDownloader


class TestTradesDownloader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_path = FIXTURES / "trades.json"
        with open(cls.fixture_path, "r", encoding="utf-8") as f:
            cls.raw_list = json.load(f)

    def test_normalize(self):
        dl = TradesDownloader()
        record = dl._normalize(self.raw_list[0], "BTC-USDT-SWAP")
        self.assertEqual(record["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(record["trade_id"], "2836584613")
        self.assertEqual(record["side"], "buy")
        self.assertEqual(str(record["px"]), "63033.6")
        self.assertEqual(str(record["sz"]), "0.06")
        self.assertIn("raw_hash", record)
        self.assertTrue(record["raw_hash"])

    def test_unique_raw_hash(self):
        dl = TradesDownloader()
        r1 = dl._normalize(self.raw_list[0], "BTC-USDT-SWAP")
        r2 = dl._normalize(self.raw_list[1], "BTC-USDT-SWAP")
        self.assertNotEqual(r1["raw_hash"], r2["raw_hash"])


class TestTradesModel(unittest.TestCase):
    def test_model(self):
        from app.db.models import Trade, TradesSyncState
        self.assertEqual(Trade.__tablename__, "trades")
        self.assertEqual(TradesSyncState.__tablename__, "trades_sync_state")


if __name__ == "__main__":
    unittest.main()
