"""时间工具函数测试（离线）"""

import unittest
from datetime import datetime, timezone

from app.utils.time_utils import (
    bar_to_seconds,
    ms_to_datetime,
    parse_date,
    utc_ms_timestamp,
)


class TestTimeUtils(unittest.TestCase):
    def test_bar_to_seconds(self):
        self.assertEqual(bar_to_seconds("1m"), 60)
        self.assertEqual(bar_to_seconds("5m"), 300)
        self.assertEqual(bar_to_seconds("1H"), 3600)
        self.assertEqual(bar_to_seconds("6H"), 21600)
        self.assertEqual(bar_to_seconds("6HUtc"), 21600)  # OKX Utc 后缀
        self.assertEqual(bar_to_seconds("1D"), 86400)
        with self.assertRaises(ValueError):
            bar_to_seconds("unknown")

    def test_utc_ms_timestamp_roundtrip(self):
        ms = utc_ms_timestamp()
        dt = ms_to_datetime(ms)
        self.assertEqual(ms, utc_ms_timestamp(dt))

    def test_ms_to_datetime_timezone(self):
        dt = ms_to_datetime(0)
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.year, 1970)

    def test_parse_date(self):
        dt = parse_date("2024-01-15")
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 15)
        # 解析结果应为 naive UTC
        self.assertIsNone(dt.tzinfo)

    def test_utc_now(self):
        from app.utils.time_utils import utc_now
        now = utc_now()
        self.assertEqual(now.tzinfo, timezone.utc)


if __name__ == "__main__":
    unittest.main()
