"""重构后布局守卫测试：防止未来误恢复旧扁平结构"""

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class TestPackageLayout(unittest.TestCase):
    def test_new_layout_dirs_exist(self):
        for p in ("app/client", "app/db", "app/proxy", "app/config", "cli"):
            self.assertTrue((ROOT / p).is_dir(), p)

    def test_old_flat_modules_removed(self):
        for p in ("app/config.py", "app/database.py", "app/db_docker.py",
                  "app/models.py", "app/okx_client.py", "app/proxy_pool.py",
                  "app/dynamic_pool.py", "app/download_scope.py", "app/conflict.py"):
            self.assertFalse((ROOT / p).exists(), p)

    def test_cli_package_has_all_entries(self):
        for p in ("cli/backfill.py", "cli/sync_continuous.py",
                  "cli/sync_realtime.py", "cli/latency_probe.py",
                  "cli/quality_report.py"):
            self.assertTrue((ROOT / p).is_file(), p)


if __name__ == "__main__":
    unittest.main()
