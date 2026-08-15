"""代理池平滑限速器节奏测试（离线，不依赖网络）"""

import threading
import time
import unittest

from app.proxy_pool import _TokenBucket


class TestTokenBucket(unittest.TestCase):
    def test_even_pacing_single_thread(self):
        """单线程串行：10 次等待应严格按 1/rate 间隔（约0.9s）"""
        tb = _TokenBucket(8)
        t0 = time.monotonic()
        for _ in range(10):
            tb.wait()
        dt = time.monotonic() - t0
        # 9 个间隔 × 0.125s ≈ 1.125s，容差放宽
        self.assertGreater(dt, 0.8)
        self.assertLess(dt, 1.6)

    def test_even_pacing_multi_thread(self):
        """8线程共享：80 次等待应平滑分布在 ~10s，无突发(最小间隔>40ms)"""
        tb = _TokenBucket(8)
        times = []
        lock = threading.Lock()

        def worker():
            for _ in range(10):
                tb.wait()
                with lock:
                    times.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        t0 = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        dt = time.monotonic() - t0

        times.sort()
        gaps = [b - a for a, b in zip(times, times[1:])]
        self.assertGreaterEqual(len(gaps), 79)
        # 总耗时接近 80/8 = 10s（宽松容差）
        self.assertGreater(dt, 8.0)
        self.assertLess(dt, 14.0)
        # 平滑发放：任意两次请求间隔不应接近0
        self.assertGreater(min(gaps), 0.04)

    def test_cooldown(self):
        """429冷却：冷却期内 wait 阻塞，冷却结束后恢复平滑"""
        tb = _TokenBucket(8)
        tb.set_cooldown(0.5)
        t0 = time.monotonic()
        tb.wait()
        dt = time.monotonic() - t0
        self.assertGreaterEqual(dt, 0.4)


if __name__ == "__main__":
    unittest.main()
