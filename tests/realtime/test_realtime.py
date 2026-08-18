"""实时模块单元测试（离线）"""

import unittest
from datetime import datetime, timezone

from app.realtime.okx_ws import OKXWebSocketClient
from app.realtime.orderbook import OrderBookHandler, OrderBookState
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
        self.assertEqual(client.url, "wss://ws.okx.com/ws/v5/public")


class TestOrderBookState(unittest.TestCase):
    def test_apply_snapshot(self):
        state = OrderBookState("BTC-USDT-SWAP")
        state.apply_snapshot({
            "instId": "BTC-USDT-SWAP",
            "bids": [["63000", "1"], ["62999", "2"]],
            "asks": [["63001", "1"], ["63002", "2"]],
            "ts": "1786862438257",
            "seqId": "100",
            "prevSeqId": "99",
        })
        self.assertEqual(state.state, "RUNNING")
        self.assertEqual(state.seq_id, 100)
        self.assertEqual(state.best_bid()[0], "63000")
        self.assertEqual(state.best_ask()[0], "63001")

    def test_apply_update(self):
        state = OrderBookState("BTC-USDT-SWAP")
        state.apply_snapshot({
            "instId": "BTC-USDT-SWAP",
            "bids": [["63000", "1"], ["62998", "1"]],
            "asks": [["63001", "1"], ["63002", "1"]],
            "ts": "1786862438257",
            "seqId": "100",
            "prevSeqId": "99",
        })
        ok = state.apply_update({
            "instId": "BTC-USDT-SWAP",
            "bids": [["62999", "2"]],
            "asks": [["63001", "0"]],
            "ts": "1786862439000",
            "seqId": "101",
            "prevSeqId": "100",
        })
        self.assertTrue(ok)
        self.assertEqual(state.seq_id, 101)
        self.assertEqual(state.best_bid()[0], "63000")  # 63000 仍优于 62999
        self.assertEqual(state.best_ask()[0], "63002")  # 原来的第二档

    def test_gap_detection(self):
        state = OrderBookState("BTC-USDT-SWAP")
        state.apply_snapshot({
            "instId": "BTC-USDT-SWAP",
            "bids": [["63000", "1"]],
            "asks": [["63001", "1"]],
            "ts": "1786862438257",
            "seqId": "100",
            "prevSeqId": "99",
        })
        ok = state.apply_update({
            "instId": "BTC-USDT-SWAP",
            "bids": [],
            "asks": [],
            "ts": "1786862439000",
            "seqId": "103",
            "prevSeqId": "102",
        })
        self.assertFalse(ok)
        self.assertEqual(state.state, "GAP_DETECTED")


class TestOrderBookHandler(unittest.TestCase):
    def test_handle_snapshot(self):
        handler = OrderBookHandler("BTC-USDT-SWAP", channel="books")
        record = handler.handle({
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "bids": [["63000", "1"]],
                "asks": [["63001", "1"]],
                "ts": "1786862438257",
                "seqId": "100",
                "prevSeqId": "99",
            }],
        })
        self.assertIsNotNone(record)
        self.assertEqual(record["snapshot_type"], "INITIAL")
        self.assertEqual(handler.state.state, "RUNNING")

    def test_ignore_other_channel(self):
        handler = OrderBookHandler("BTC-USDT-SWAP")
        record = handler.handle({
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [{}],
        })
        self.assertIsNone(record)


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
        from app.db.database import get_engine
        from sqlalchemy import text
        with get_engine().begin() as conn:
            conn.execute(
                text("DELETE FROM trades WHERE inst_id = 'TEST-USDT-SWAP' AND trade_id = 'test-123'"),
            )


if __name__ == "__main__":
    unittest.main()
