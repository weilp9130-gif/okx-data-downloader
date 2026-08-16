"""Instruments 模块单元测试（离线，基于 fixture）"""

import json
import os
import unittest
from pathlib import Path

from app.downloader.instruments import normalize_instrument


class TestInstrumentNormalize(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_path = Path(__file__).resolve().parent / "fixtures" / "instruments.json"
        with open(cls.fixture_path, "r", encoding="utf-8") as f:
            cls.raw_list = json.load(f)

    def test_normalize_basic(self):
        record = normalize_instrument(self.raw_list[0])
        self.assertEqual(record["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(record["inst_type"], "SWAP")
        self.assertEqual(record["base_ccy"], "BTC")
        self.assertEqual(record["quote_ccy"], "USDT")
        self.assertEqual(record["settle_ccy"], "USDT")
        self.assertEqual(str(record["ct_val"]), "0.01")
        self.assertEqual(str(record["tick_sz"]), "0.1")
        self.assertEqual(record["state"], "live")
        self.assertIsNotNone(record["list_time"])
        self.assertIsNone(record["exp_time"])
        self.assertIsNotNone(record["raw_json"])

    def test_normalize_invalid(self):
        self.assertIsNone(normalize_instrument({}))
        self.assertIsNone(normalize_instrument({"instId": "X"}))
        self.assertIsNone(normalize_instrument({"instType": "SWAP"}))

    def test_normalize_all_fixtures(self):
        for raw in self.raw_list:
            record = normalize_instrument(raw)
            self.assertIsNotNone(record)
            self.assertTrue(record["inst_id"])
            self.assertTrue(record["inst_type"])


class TestInstrumentImport(unittest.TestCase):
    def test_model_import(self):
        from app.models import Instrument
        self.assertEqual(Instrument.__tablename__, "instruments")

    def test_downloader_import(self):
        from app.downloader.instruments import InstrumentDownloader
        self.assertTrue(InstrumentDownloader)


if __name__ == "__main__":
    unittest.main()
