"""任务注册表测试：TaskSpec 校验（恶意/缺失）、argv 构造、capability/rate_group"""

import sys
import unittest
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError

from app.task import registry
from app.task.registry import (
    REGISTRY,
    get_spec,
    validate_params,
)


class TestRegistryCatalog(unittest.TestCase):
    def test_all_plan_task_types_registered(self):
        expected = {
            "KLINE", "TRADES", "FUNDING_RATE", "MARK_PRICE", "INDEX_PRICE",
            "OPEN_INTEREST", "INSTRUMENTS", "LATENCY_PROBE", "QUALITY_CHECK",
            "ASSET_REFRESH",
        }
        # 测试模块可能注册 FAKE 等测试专用类型
        self.assertEqual(set(REGISTRY) - {"FAKE"}, expected)

    def test_capability_and_rate_group(self):
        self.assertEqual(get_spec("KLINE").capability, "download")
        self.assertEqual(get_spec("KLINE").rate_group, "okx_market")
        self.assertEqual(get_spec("LATENCY_PROBE").capability, "latency")
        self.assertEqual(get_spec("LATENCY_PROBE").rate_group, "okx_ws")
        self.assertIsNone(get_spec("ASSET_REFRESH").rate_group)


class TestParamValidation(unittest.TestCase):
    def test_kline_valid(self):
        params = validate_params("KLINE", {
            "inst": "BTC-USDT-SWAP",
            "bars": ["1m", "1H"],
            "start": "2024-01-01",
            "end": "2024-02-01",
        })
        self.assertEqual(params["strategy"], "patch")
        self.assertEqual(params["bars"], ["1m", "1H"])

    def test_kline_extra_field_rejected(self):
        with self.assertRaises(ValidationError):
            validate_params("KLINE", {
                "inst": "BTC-USDT-SWAP",
                "bars": ["1m"],
                "start": "2024-01-01",
                "end": "2024-02-01",
                "evil": "; rm -rf /",
            })

    def test_kline_invalid_bar_rejected(self):
        with self.assertRaises(ValidationError):
            validate_params("KLINE", {
                "inst": "BTC-USDT-SWAP",
                "bars": ["99Z"],
                "start": "2024-01-01",
                "end": "2024-02-01",
            })

    def test_kline_bad_date_rejected(self):
        with self.assertRaises(ValidationError):
            validate_params("KLINE", {
                "inst": "BTC-USDT-SWAP",
                "bars": ["1m"],
                "start": "not-a-date",
                "end": "2024-02-01",
            })

    def test_injection_via_inst_rejected(self):
        with self.assertRaises(ValidationError):
            validate_params("TRADES", {
                "inst": "BTC-USDT-SWAP; echo hacked",
                "start": "2024-01-01",
                "end": "2024-02-01",
            })

    def test_max_pages_range(self):
        with self.assertRaises(ValidationError):
            validate_params("TRADES", {
                "inst": "BTC-USDT-SWAP",
                "start": "2024-01-01",
                "end": "2024-02-01",
                "max_pages": 0,
            })
        with self.assertRaises(ValidationError):
            validate_params("TRADES", {
                "inst": "BTC-USDT-SWAP",
                "start": "2024-01-01",
                "end": "2024-02-01",
                "max_pages": 101,
            })

    def test_latency_channels_whitelist(self):
        with self.assertRaises(ValidationError):
            validate_params("LATENCY_PROBE", {
                "insts": ["BTC-USDT-SWAP"],
                "channels": ["not-a-channel"],
                "duration": 60,
            })

    def test_strategy_enum(self):
        with self.assertRaises(ValidationError):
            validate_params("KLINE", {
                "inst": "BTC-USDT-SWAP",
                "bars": ["1m"],
                "start": "2024-01-01",
                "end": "2024-02-01",
                "strategy": "nuke-all",
            })

    def test_unknown_task_type(self):
        with self.assertRaises(ValueError):
            validate_params("NOPE", {})


class TestArgvConstruction(unittest.TestCase):
    def test_kline_argv(self):
        spec = get_spec("KLINE")
        params = validate_params("KLINE", {
            "inst": "BTC-USDT-SWAP",
            "bars": ["1m", "1H"],
            "start": "2024-01-01",
            "end": "2024-02-01",
        })
        argv = spec.command_argv(params, "runtime/jobs/x/attempt-1.jsonl")
        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(argv[1:3], ["-m", "cli.backfill"])
        self.assertIn("--inst", argv)
        self.assertEqual(argv[argv.index("--inst") + 1], "BTC-USDT-SWAP")
        self.assertEqual(argv[argv.index("--bar") + 1], "1m,1H")
        self.assertNotIn("--overwrite", argv)

    def test_kline_full_strategy_adds_overwrite(self):
        spec = get_spec("KLINE")
        params = validate_params("KLINE", {
            "inst": "BTC-USDT-SWAP",
            "bars": ["1D"],
            "start": "2024-01-01",
            "end": "2024-02-01",
            "strategy": "full",
        })
        argv = spec.command_argv(params, "runtime/jobs/x/attempt-1.jsonl")
        self.assertIn("--overwrite", argv)

    def test_kline_incremental_shrinks_window(self):
        spec = get_spec("KLINE")
        params = validate_params("KLINE", {
            "inst": "BTC-USDT-SWAP",
            "bars": ["1D"],
            "start": "2020-01-01",
            "end": "2025-01-01",
            "strategy": "incremental",
        })
        argv = spec.command_argv(params, "p.jsonl")
        start = argv[argv.index("--start") + 1]
        end = argv[argv.index("--end") + 1]
        now = datetime.now(timezone.utc)
        expected_start = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        expected_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(start, expected_start.strftime("%Y-%m-%d"))
        self.assertEqual(end, expected_end.strftime("%Y-%m-%d"))

    def test_trades_argv_max_pages(self):
        spec = get_spec("TRADES")
        params = validate_params("TRADES", {
            "inst": "BTC-USDT-SWAP",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "max_pages": 5,
        })
        argv = spec.command_argv(params, "p.jsonl")
        self.assertEqual(argv[argv.index("--max-pages") + 1], "5")

    def test_latency_probe_argv(self):
        spec = get_spec("LATENCY_PROBE")
        params = validate_params("LATENCY_PROBE", {
            "insts": ["BTC-USDT-SWAP"],
            "channels": ["trades", "bbo-tbt"],
            "duration": 120,
        })
        argv = spec.command_argv(params, "p.jsonl")
        self.assertEqual(argv[1:3], ["-m", "cli.latency_probe"])
        self.assertEqual(argv[argv.index("--insts") + 1], "BTC-USDT-SWAP")
        self.assertEqual(argv[argv.index("--channels") + 1], "trades,bbo-tbt")
        self.assertEqual(argv[argv.index("--duration") + 1], "120")
        self.assertIn("--summary-interval", argv)

    def test_quality_check_argv_output_json(self):
        spec = get_spec("QUALITY_CHECK")
        params = validate_params("QUALITY_CHECK", {
            "inst": "BTC-USDT-SWAP",
            "bar": "1D",
            "cross_source": True,
        })
        argv = spec.command_argv(params, "runtime/jobs/x/attempt-1.jsonl")
        self.assertEqual(argv[argv.index("--output") + 1],
                         "runtime/jobs/x/attempt-1.json")
        self.assertIn("--cross-source", argv)

    def test_no_shell(self):
        valid_params = {
            "KLINE": {"inst": "BTC-USDT-SWAP", "bars": ["1m"],
                      "start": "2024-01-01", "end": "2024-02-01"},
            "TRADES": {"inst": "BTC-USDT-SWAP",
                       "start": "2024-01-01", "end": "2024-02-01"},
            "FUNDING_RATE": {"inst": "BTC-USDT-SWAP",
                             "start": "2024-01-01", "end": "2024-02-01"},
            "MARK_PRICE": {"inst": "BTC-USDT-SWAP", "bar": "1D",
                           "start": "2024-01-01", "end": "2024-02-01"},
            "INDEX_PRICE": {"inst": "BTC-USDT-SWAP", "bar": "1D",
                            "start": "2024-01-01", "end": "2024-02-01"},
            "OPEN_INTEREST": {"inst": "BTC-USDT-SWAP"},
            "INSTRUMENTS": {"inst_type": "SWAP"},
            "LATENCY_PROBE": {"insts": ["BTC-USDT-SWAP"],
                              "channels": ["trades"], "duration": 60},
            "QUALITY_CHECK": {"inst": "BTC-USDT-SWAP", "bar": "1D"},
            "ASSET_REFRESH": {"scope": "all", "mode": "incremental"},
        }
        for task_type, params in valid_params.items():
            spec = get_spec(task_type)
            argv = spec.command_argv(validate_params(task_type, params), "p.jsonl")
            self.assertEqual(argv[0], sys.executable, task_type)
            self.assertNotEqual(argv[0], "sh")
            self.assertNotEqual(argv[0], "cmd")


if __name__ == "__main__":
    unittest.main()
