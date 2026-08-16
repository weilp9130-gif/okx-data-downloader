"""实时模块单元测试（离线）"""

import unittest
from datetime import datetime, timezone

from app.realtime.okx_ws import OKXWebSocketClient
from app.realtime.trades import TradesRealtimeHandler
from app.realtime.writer import TradeWriter


class MockWriter:
    def __init__(self):
        self.items = []

    def put(self, item: dict) -> bool:
        self.items.append(item)
        return True

    def stop(self, timeout: float = 5.0) -> None:
        pass


class TestTradesRealtimeHandler(unittest.TestCase):
    def test_handle_trade(self):
        writer = MockWriter()
        handler = TradesRealtimeHandler(writer)
        data = {
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "12345",
                    "ts": "1786862438257",
                    "px": "63033.6",
                    "sz": "0.06",
                    "side": "buy",
                }
            ],
        }
        count = handler.handle(data)
        self.assertEqual(count, 1)
        self.assertEqual(len(writer.items), 1)
        item = writer.items[0]
        self.assertEqual(item["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(item["trade_id"], "12345")
        self.assertEqual(item["side"], "buy")
        self.assertEqual(item["source"], "WS")
        self.assertIsInstance(item["received_at"], datetime)

    def test_ignore_other_channel(self):
        writer = MockWriter()
        handler = TradesRealtimeHandler(writer)
        data = {
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [{"last": "63000"}],
        }
        count = handler.handle(data)
        self.assertEqual(count, 0)
        self.assertEqual(len(writer.items), 0)


class TestOKXWebSocketClient(unittest.TestCase):
    def test_client_init(self):
        client = OKXWebSocketClient()
        self.assertEqual(client.url, "wss://ws.okx.com:8443/ws/v5/public")


class TestTradeWriter(unittest.TestCase):
    def test_writer_put(self):
        writer = TradeWriter()
        ok = writer.put({
            "inst_id": "TEST-USDT-SWAP",
            "trade_id": "test-123",
            "ts": datetime.now(timezone.utc),
            "px": "63000",
            "sz": "0.1",
            "side": "buy",
            "received_at": datetime.now(timezone.utc),
            "raw_json": {},
        })
        self.assertTrue(ok)
        writer.stop()
        # 清理测试数据，避免污染后续恢复逻辑
        from app.database import get_engine
        from sqlalchemy import text
        with get_engine().begin() as conn:
            conn.execute(
                text("DELETE FROM trades WHERE inst_id = 'TEST-USDT-SWAP' AND trade_id = 'test-123'"),
            )


if __name__ == "__main__":
    unittest.main()
