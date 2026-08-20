"""质量评分测试：报告 fixture → 四维评分"""

import unittest

from app.services.quality_score import score_data_type, score_report

# 干净报告：无重复/无空值/跨源一致/数据新鲜
CLEAN_REPORT = {
    "inst_id": "BTC-USDT-SWAP",
    "bar": "1D",
    "reports": [
        {
            "data_type": "candles",
            "level1": {"candles": {"total": 1000, "duplicate": 0}},
            "level2": {
                "candles": {
                    "nulls": 0, "invalid_price": 0, "timestamp_regression": 0,
                    "min_ts": "2026-08-01T00:00:00+00:00",
                    "max_ts": "2026-08-18T00:00:00+00:00",
                }
            },
            "level3": {},
        },
        {
            "data_type": "trades",
            "level1": {"trades": {"total": 1000, "duplicate": 0}},
            "level2": {"trades": {"nulls": 0, "invalid_price_size": 0}},
            "level3": {},
        },
    ],
    "cross_source_volume": {"buckets_checked": 17, "max_ratio": 0.0},
}

# 脏报告：重复/空值/回归/跨源不一致
DIRTY_REPORT = {
    "inst_id": "BTC-USDT-SWAP",
    "bar": "1D",
    "reports": [
        {
            "data_type": "candles",
            "level1": {"candles": {"total": 1000, "duplicate": 100}},
            "level2": {
                "candles": {
                    "nulls": 50, "invalid_price": 10, "timestamp_regression": 5,
                    "min_ts": "2026-08-01T00:00:00+00:00",
                    "max_ts": "2026-08-18T00:00:00+00:00",
                }
            },
            "level3": {},
        }
    ],
    "cross_source_volume": {"buckets_checked": 17, "max_ratio": 0.5},
}


class TestScoreReport(unittest.TestCase):
    def test_clean_report_high_score(self):
        result = score_report(CLEAN_REPORT)
        self.assertIn("KLINE", result["scores"])
        self.assertIn("TRADES", result["scores"])
        kline = result["scores"]["KLINE"]
        self.assertGreaterEqual(kline["score"], 90)
        self.assertAlmostEqual(kline["completeness"], 1.0, places=4)
        self.assertAlmostEqual(kline["validity"], 1.0, places=4)
        self.assertAlmostEqual(kline["consistency"], 1.0, places=4)

    def test_dirty_report_penalized(self):
        result = score_report(DIRTY_REPORT)
        kline = result["scores"]["KLINE"]
        self.assertLess(kline["score"], 90)
        # duplicate=100, nulls=50, invalid=10, regression=5 → validity = 1 - 165/1000
        self.assertAlmostEqual(kline["validity"], 1.0 - 165 / 1000, places=4)
        # consistency = 1 - 0.5
        self.assertAlmostEqual(kline["consistency"], 0.5, places=4)
        # completeness 按 span/interval：17 天 → 18 根，total=1000 ≥ 18 → missing=0
        self.assertAlmostEqual(kline["completeness"], 1.0, places=4)

    def test_empty_total_gives_full_marks(self):
        report = {
            "reports": [{
                "data_type": "oi",
                "level1": {"open_interest": {"total": 0, "duplicate": 0}},
                "level2": {"open_interest": {"invalid_oi": 0}},
                "level3": {},
            }]
        }
        sc = score_report(report)["scores"]["OPEN_INTEREST"]
        self.assertEqual(sc["validity"], 1.0)
        self.assertEqual(sc["completeness"], 1.0)

    def test_instruments_excluded(self):
        report = {
            "reports": [
                {"data_type": "instruments",
                 "level1": {"instruments": {"total": 500}}},
                {"data_type": "funding",
                 "level1": {"funding_rates": {"total": 100, "duplicate": 0}},
                 "level2": {"funding_rates": {"nulls": 0}},
                 "level3": {},
                },
            ]
        }
        result = score_report(report)
        self.assertNotIn("INSTRUMENTS", result["scores"])
        self.assertIn("FUNDING_RATE", result["scores"])

    def test_score_data_type_missing_cross_source_defaults_1(self):
        report = {
            "data_type": "candles",
            "level1": {"candles": {"total": 100, "duplicate": 0}},
            "level2": {"candles": {"nulls": 0, "invalid_price": 0,
                                   "timestamp_regression": 0}},
            "level3": {},
        }
        sc = score_data_type(report, "KLINE")
        self.assertEqual(sc["consistency"], 1.0)
        self.assertEqual(sc["freshness"], 1.0)

    def test_score_bounds(self):
        result = score_report(DIRTY_REPORT)
        for dataset, sc in result["scores"].items():
            self.assertGreaterEqual(sc["score"], 0)
            self.assertLessEqual(sc["score"], 100)


if __name__ == "__main__":
    unittest.main()
