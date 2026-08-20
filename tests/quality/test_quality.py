"""数据质量验证单元测试"""

import unittest
import pytest

pytestmark = pytest.mark.integration

from app.quality.validator import DataQualityValidator


class TestDataQualityValidator(unittest.TestCase):
    def test_validate_instruments(self):
        v = DataQualityValidator()
        report = v.validate("instruments", "BTC-USDT-SWAP")
        self.assertIn("level1", report)
        self.assertIn("instruments", report["level1"])
        self.assertIn("total", report["level1"]["instruments"])

    def test_validate_mark_prices(self):
        v = DataQualityValidator()
        report = v.validate("mark", "BTC-USDT-SWAP", "1D")
        self.assertIn("level1", report)
        self.assertIn("mark_prices", report["level1"])
        self.assertIn("level2", report)
        self.assertIn("mark_prices", report["level2"])

    def test_validate_trades(self):
        v = DataQualityValidator()
        report = v.validate("trades", "BTC-USDT-SWAP")
        self.assertIn("level1", report)
        self.assertIn("trades", report["level1"])


if __name__ == "__main__":
    unittest.main()
