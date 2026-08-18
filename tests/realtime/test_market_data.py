"""Phase 7 市场数据处理器单元测试"""

import unittest

from app.realtime.market_data import MarketDataHandler


class TestMarketDataHandler(unittest.TestCase):
    def setUp(self):
        self.handler = MarketDataHandler()

    def test_open_interest(self):
        records = self.handler.handle({
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "oi": "3364343",
                "oiCcy": "33643",
                "oiUsd": "2120589148",
                "ts": "1786861970979",
            }],
        })
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target"], "open_interest_realtime")
        rec = records[0]["record"]
        self.assertEqual(rec["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(rec["oi"], "3364343")

    def test_funding_rate(self):
        records = self.handler.handle({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "fundingRate": "0.0001",
                "realizedRate": "0.0001",
                "fundingTime": "1786861970979",
                "method": "current_period",
            }],
        })
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target"], "funding_rates")
        rec = records[0]["record"]
        self.assertEqual(rec["inst_id"], "BTC-USDT-SWAP")
        self.assertIsNotNone(rec["funding_time"])
        self.assertNotIn("raw_json", rec)

    def test_mark_price(self):
        records = self.handler.handle({
            "arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "markPx": "62987.9",
                "ts": "1786861970979",
            }],
        })
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target"], "mark_prices")
        rec = records[0]["record"]
        self.assertEqual(rec["bar"], "realtime")
        self.assertEqual(rec["o"], "62987.9")
        self.assertEqual(rec["h"], "62987.9")
        self.assertEqual(rec["l"], "62987.9")
        self.assertEqual(rec["c"], "62987.9")

    def test_index_tickers(self):
        records = self.handler.handle({
            "arg": {"channel": "index-tickers", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "idxPx": "63005.5",
                "ts": "1786861970979",
            }],
        })
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target"], "index_prices")
        self.assertEqual(records[0]["record"]["c"], "63005.5")

    def test_candles_with_inst(self):
        from datetime import datetime, timezone
        records = self.handler.handle_candles_with_inst(
            {"data": [["1786861970979", "63000", "63100", "62900", "63050", "100", "0.001", "63000", "0"]]},
            bar="1m",
            inst_id="BTC-USDT-SWAP",
            received_at=datetime.now(timezone.utc),
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target"], "candles")
        rec = records[0]["record"]
        self.assertEqual(rec["bar"], "1m")
        self.assertEqual(rec["o"], "63000")
        self.assertEqual(rec["vol"], "100")

    def test_unknown_channel(self):
        records = self.handler.handle({
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [{"last": "63000"}],
        })
        self.assertEqual(records, [])

    def test_ignore_subscribe_event(self):
        records = self.handler.handle({
            "event": "subscribe",
            "arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
        })
        self.assertEqual(records, [])


class TestMarketDataModel(unittest.TestCase):
    def test_model(self):
        from app.db.models import OpenInterestRealtime
        self.assertEqual(OpenInterestRealtime.__tablename__, "open_interest_realtime")


if __name__ == "__main__":
    unittest.main()
