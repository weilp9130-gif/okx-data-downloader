"""Phase 2 单元测试（离线，基于 fixture）"""

import json
import unittest

from tests.path_utils import FIXTURES

from app.downloader.index_price import IndexPriceDownloader
from app.downloader.mark_price import MarkPriceDownloader
from app.downloader.open_interest import normalize_open_interest


class TestMarkPriceNormalize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_path = FIXTURES / "mark_price_candles.json"
        with open(cls.fixture_path, "r", encoding="utf-8") as f:
            cls.raw_list = json.load(f)

    def test_parse(self):
        self.assertEqual(len(self.raw_list), 3)
        first = self.raw_list[0]
        self.assertEqual(first[0], "1786809600000")
        self.assertEqual(first[1], "63065")


class TestIndexPriceNormalize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_path = FIXTURES / "index_candles.json"
        with open(cls.fixture_path, "r", encoding="utf-8") as f:
            cls.raw_list = json.load(f)

    def test_parse(self):
        self.assertEqual(len(self.raw_list), 3)
        first = self.raw_list[0]
        self.assertEqual(first[0], "1786809600000")


class TestOpenInterestNormalize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_path = FIXTURES / "open_interest.json"
        with open(cls.fixture_path, "r", encoding="utf-8") as f:
            cls.raw_list = json.load(f)

    def test_normalize(self):
        record = normalize_open_interest(self.raw_list[0])
        self.assertEqual(record["inst_id"], "BTC-USDT-SWAP")
        self.assertIn("oi", record)
        self.assertIn("oi_ccy", record)
        self.assertIn("oi_usd", record)

    def test_normalize_invalid(self):
        self.assertIsNone(normalize_open_interest({}))
        self.assertIsNone(normalize_open_interest({"instId": "BTC-USDT-SWAP"}))


class TestPhase2ModelImport(unittest.TestCase):
    def test_models(self):
        from app.db.models import (
            OpenInterest,
            MarkPrice,
            IndexPrice,
            OISyncState,
            MarkPriceSyncState,
            IndexPriceSyncState,
            FundingSyncState,
            MarketDataProvenance,
        )
        self.assertEqual(OpenInterest.__tablename__, "open_interest")
        self.assertEqual(MarkPrice.__tablename__, "mark_prices")
        self.assertEqual(IndexPrice.__tablename__, "index_prices")


class TestDownloadersImport(unittest.TestCase):
    def test_downloaders(self):
        from app.downloader import (
            MarkPriceDownloader,
            IndexPriceDownloader,
            OpenInterestDownloader,
        )
        self.assertTrue(MarkPriceDownloader)
        self.assertTrue(IndexPriceDownloader)
        self.assertTrue(OpenInterestDownloader)


if __name__ == "__main__":
    unittest.main()
