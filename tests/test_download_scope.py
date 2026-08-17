# -*- coding: utf-8 -*-
import unittest
import os
from datetime import datetime, timedelta, timezone

from app.download_scope import (
    load_scope,
    resolve_instruments,
    resolve_time_range,
    scope_default,
    _DEFAULTS,
)


class _FakeClient:
    def __init__(self, data):
        self._data = data

    def get_instruments(self, inst_type="SWAP"):
        return [d for d in self._data if d.get("instType") == inst_type]


_FAKE_INSTS = [
    {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "settleCcy": "USDT", "state": "live"},
    {"instType": "SWAP", "instId": "ETH-USDT-SWAP", "settleCcy": "USDT", "state": "live"},
    {"instType": "SWAP", "instId": "BTC-USD-SWAP", "settleCcy": "USD", "state": "live"},
    {"instType": "SWAP", "instId": "DEAD-USDT-SWAP", "settleCcy": "USDT", "state": "suspend"},
]


class TestLoadScope(unittest.TestCase):
    def test_defaults_when_file_missing(self):
        scope = load_scope("C:/nope/missing_scope.toml")
        self.assertEqual(scope["inst_region"]["mode"], "all")
        self.assertEqual(scope["time_region"]["limit_days"], 0)
        self.assertEqual(scope["defaults"]["bar"], "1m")

    def test_scope_file_present(self):
        scope = load_scope()
        self.assertIn("inst_region", scope)
        self.assertIn("time_region", scope)
        self.assertIn("defaults", scope)
        self.assertEqual(scope["inst_region"]["inst_type"], "SWAP")


class TestResolveInstruments(unittest.TestCase):
    def test_include_mode(self):
        scope = {
            "inst_region": {"mode": "include", "include": ["BTC-USDT-SWAP"], "exclude": []}
        }
        self.assertEqual(resolve_instruments(scope, client=None, explicit=None),
                         ["BTC-USDT-SWAP"])

    def test_explicit_overrides(self):
        scope = {"inst_region": {"mode": "include", "include": ["BTC-USDT-SWAP"], "exclude": []}}
        self.assertEqual(resolve_instruments(scope, client=None, explicit=["ETH-USDT-SWAP"]),
                         ["ETH-USDT-SWAP"])

    def test_all_mode_filters(self):
        scope = {
            "inst_region": {
                "mode": "all", "include": [], "exclude": [],
                "inst_type": "SWAP", "settle_ccy": "USDT", "state": "live",
            }
        }
        result = resolve_instruments(scope, client=_FakeClient(_FAKE_INSTS))
        self.assertEqual(result, ["BTC-USDT-SWAP", "ETH-USDT-SWAP"])

    def test_exclude_mode(self):
        scope = {
            "inst_region": {
                "mode": "exclude", "include": [], "exclude": ["ETH-USDT-SWAP"],
                "inst_type": "SWAP", "settle_ccy": "USDT", "state": "live",
            }
        }
        result = resolve_instruments(scope, client=_FakeClient(_FAKE_INSTS))
        self.assertEqual(result, ["BTC-USDT-SWAP"])


class TestResolveTimeRange(unittest.TestCase):
    def test_explicit_args_priority(self):
        scope = {"time_region": {"start": "2024-01-01", "end": "2024-02-01", "limit_days": 0}}
        start, end = resolve_time_range(scope, start="2023-01-01", end="2023-02-01")
        self.assertEqual(start.year, 2023)
        self.assertEqual(end.year, 2023)

    def test_scope_config_used(self):
        scope = {"time_region": {"start": "2024-01-01", "end": "2024-02-01", "limit_days": 0}}
        start, end = resolve_time_range(scope)
        self.assertEqual(start.month, 1)
        self.assertEqual(end.month, 2)

    def test_limit_days(self):
        scope = {"time_region": {"start": "", "end": "", "limit_days": 7}}
        start, end = resolve_time_range(scope)
        self.assertAlmostEqual((end - start).total_seconds(), 7 * 86400, delta=5)

    def test_empty_returns_none_start(self):
        scope = {"time_region": {"start": "", "end": "", "limit_days": 0}}
        start, end = resolve_time_range(scope)
        self.assertIsNone(start)
        self.assertIsNotNone(end)


class TestScopeDefault(unittest.TestCase):
    def test_returns_value(self):
        scope = {"defaults": {"bar": "5m"}}
        self.assertEqual(scope_default(scope, "bar", "1m"), "5m")

    def test_fallback_on_empty(self):
        scope = {"defaults": {"bar": ""}}
        self.assertEqual(scope_default(scope, "bar", "1m"), "1m")


if __name__ == "__main__":
    unittest.main()
