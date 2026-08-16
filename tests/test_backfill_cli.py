"""Phase 9 回归测试：backfill CLI 参数解析 / dry-run 估算 / 质量问题收集"""

import unittest
from datetime import datetime, timedelta, timezone

import backfill
from app.quality.validator import DataQualityValidator


class _Args:
    """模拟 argparse.Namespace"""

    def __init__(self, **kw):
        self.data_type = kw.get("data_type", "all")
        self.inst = kw.get("inst", "BTC-USDT-SWAP")
        self.index_inst = kw.get("index_inst")
        self.inst_type = kw.get("inst_type", "SWAP")
        self.bar = kw.get("bar", "1D")
        self.start = kw.get("start")
        self.end = kw.get("end")
        self.limit_days = kw.get("limit_days")
        self.max_pages = kw.get("max_pages", 10)
        self.allow_large = kw.get("allow_large", False)
        self.dry_run = kw.get("dry_run", False)


class TestResolveTypes(unittest.TestCase):
    def test_single_type(self):
        self.assertEqual(backfill._resolve_types(_Args(data_type="mark")), ["mark"])

    def test_all_excludes_large_by_default(self):
        types = backfill._resolve_types(_Args(data_type="all"))
        self.assertNotIn("trades", types)
        self.assertNotIn("trade_aggregates", types)
        self.assertIn("instruments", types)
        self.assertIn("funding", types)

    def test_all_includes_large_when_allowed(self):
        types = backfill._resolve_types(_Args(data_type="all", allow_large=True))
        self.assertIn("trades", types)
        self.assertIn("trade_aggregates", types)


class TestResolveTimeRange(unittest.TestCase):
    def test_start_end_priority(self):
        start, end = backfill._resolve_time_range(
            _Args(start="2024-01-01", end="2024-02-01", limit_days=999)
        )
        self.assertEqual(start.year, 2024)
        self.assertEqual(start.month, 1)
        self.assertEqual(end.month, 2)

    def test_limit_days(self):
        start, end = backfill._resolve_time_range(_Args(limit_days=3))
        self.assertAlmostEqual((end - start).total_seconds(), 3 * 86400, delta=5)

    def test_default_one_year(self):
        start, end = backfill._resolve_time_range(_Args())
        self.assertEqual(end.year - start.year, 1)


class TestResolveIndexInst(unittest.TestCase):
    def test_derive_from_swap(self):
        self.assertEqual(
            backfill._resolve_index_inst(_Args(inst="BTC-USDT-SWAP")), "BTC-USDT"
        )

    def test_explicit_index_inst(self):
        self.assertEqual(
            backfill._resolve_index_inst(
                _Args(inst="BTC-USDT-SWAP", index_inst="ETH-USDT")
            ),
            "ETH-USDT",
        )

    def test_spot_unchanged(self):
        self.assertEqual(backfill._resolve_index_inst(_Args(inst="BTC-USDT")), "BTC-USDT")


class TestDryRunEstimate(unittest.TestCase):
    def setUp(self):
        self.end = datetime(2026, 8, 16, tzinfo=timezone.utc)
        self.start = self.end - timedelta(days=10)

    def test_mark_estimate(self):
        est = backfill._estimate("mark", _Args(bar="1D"), self.start, self.end)
        self.assertEqual(est["rows"], 10)
        self.assertEqual(est["requests"], 1)

    def test_funding_estimate(self):
        est = backfill._estimate("funding", _Args(), self.start, self.end)
        # 10 天 / 8 小时 = 30 次结算
        self.assertEqual(est["rows"], 30)

    def test_trades_estimate_respects_max_pages(self):
        est = backfill._estimate("trades", _Args(max_pages=5), self.start, self.end)
        self.assertEqual(est["requests"], 5)
        self.assertEqual(est["rows"], 5 * backfill.PAGE_LIMIT)

    def test_oi_estimate_single_snapshot(self):
        est = backfill._estimate("oi", _Args(), self.start, self.end)
        self.assertEqual(est["rows"], 1)

    def test_trade_aggregates_no_requests(self):
        est = backfill._estimate("trade_aggregates", _Args(bar="1m"), self.start, self.end)
        self.assertEqual(est["requests"], 0)
        self.assertEqual(est["rows"], 10 * 24 * 60)


class TestCollectIssues(unittest.TestCase):
    def test_clean_report_has_no_issues(self):
        v = DataQualityValidator()
        report = {
            "data_type": "mark",
            "level1": {"mark_prices": {"total": 31, "duplicate": 0}},
            "level2": {"mark_prices": {"nulls": 0, "invalid_price": 0, "timestamp_regression": 0}},
            "level3": {},
        }
        self.assertEqual(v.collect_issues(report), [])

    def test_detects_duplicates_and_nulls(self):
        v = DataQualityValidator()
        report = {
            "data_type": "trades",
            "level1": {"trades": {"total": 100, "duplicate": 3}},
            "level2": {"trades": {"nulls": 2, "invalid_price_size": 0, "duplicate_trade_id": 1}},
            "level3": {},
        }
        issues = v.collect_issues(report)
        self.assertEqual(len(issues), 3)
        self.assertTrue(any("duplicate=3" in i for i in issues))
        self.assertTrue(any("nulls=2" in i for i in issues))
        self.assertTrue(any("duplicate_trade_id=1" in i for i in issues))

    def test_ignores_non_issue_metrics(self):
        v = DataQualityValidator()
        report = {
            "data_type": "mark",
            "level1": {"mark_prices": {"total": 999, "duplicate": 0}},
            "level2": {"mark_prices": {"min_ts": "2024-01-01T00:00:00+00:00"}},
            "level3": {},
        }
        self.assertEqual(v.collect_issues(report), [])


if __name__ == "__main__":
    unittest.main()
