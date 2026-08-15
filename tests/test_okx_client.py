"""OKXClient 单元测试（离线）"""

import unittest
import time

from app.okx_client import OKXClient


class TestOKXClient(unittest.TestCase):
    def test_timeout_and_retries_from_config(self):
        client = OKXClient()
        self.assertGreaterEqual(client.TIMEOUT, 1)
        self.assertGreaterEqual(client.MAX_RETRIES, 1)

    def test_429_rate_drop_and_recovery_state(self):
        """429 后速率下降，并设置恢复时间"""
        client = OKXClient()
        initial_rate = client._rate
        client._on_429()
        self.assertLess(client._rate, initial_rate)
        self.assertGreater(client._recover_at, time.monotonic())


if __name__ == "__main__":
    unittest.main()
