"""IP代理池单元测试（离线）"""

import threading
import time
import unittest

from app import proxy_pool as proxy_pool_module
from app.proxy_pool import ProxyPool


class TestProxyPoolAcquire(unittest.TestCase):
    def test_acquire_sleep_does_not_hold_lock(self):
        """空代理池下 acquire 等待时不应持有锁阻塞其他线程"""
        original_sleep = proxy_pool_module.ACQUIRE_NONE_SLEEP
        original_stall = proxy_pool_module.STALL_MAX_NONE
        proxy_pool_module.ACQUIRE_NONE_SLEEP = 0.05
        proxy_pool_module.STALL_MAX_NONE = 3
        try:
            pool = ProxyPool(proxies=[])
            results = []
            errors = []

            def worker():
                try:
                    pool.acquire(key="test")
                    results.append("ok")
                except RuntimeError:
                    results.append("err")
                except Exception as e:
                    errors.append(str(e))

            t0 = time.monotonic()
            threads = [threading.Thread(target=worker) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            dt = time.monotonic() - t0

            # 5 个线程如果串行 sleep 需要 > 0.75s；锁外 sleep 应 < 0.2s
            self.assertLess(dt, 0.3, "acquire 似乎在锁内 sleep，并发被阻塞")
            self.assertEqual(len(results), 5)
            self.assertEqual(errors, [])
        finally:
            proxy_pool_module.ACQUIRE_NONE_SLEEP = original_sleep
            proxy_pool_module.STALL_MAX_NONE = original_stall


if __name__ == "__main__":
    unittest.main()
