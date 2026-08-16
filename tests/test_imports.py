"""模块可导入性冒烟测试（确保重构后依赖关系正确）"""

import unittest


class TestImports(unittest.TestCase):
    def test_app_package(self):
        import app  # noqa: F401
        from app.config import Config
        self.assertTrue(Config)

    def test_core_modules(self):
        from app import database, db_docker, models, okx_client, proxy_pool  # noqa: F401
        from app.dynamic_pool import RUNTIME_DIR, LISTENERS_FILE
        from pathlib import Path
        self.assertEqual(RUNTIME_DIR.name, "runtime")
        self.assertEqual(LISTENERS_FILE.name, "mihomo_listeners.yaml")

    def test_sub_packages(self):
        from app.downloader import (
            CandleDownloader,
            FundingRateDownloader,
            IndexPriceDownloader,
            InstrumentDownloader,
            MarkPriceDownloader,
            OpenInterestDownloader,
            TradesDownloader,
        )  # noqa: F401
        from app.utils import get_logger, bar_to_seconds, ms_to_datetime  # noqa: F401
        self.assertEqual(bar_to_seconds("1m"), 60)

    def test_log_path(self):
        from app.utils.logger import LOG_DIR
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        self.assertEqual(LOG_DIR, root / "logs")


if __name__ == "__main__":
    unittest.main()
