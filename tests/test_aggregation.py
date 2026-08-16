"""Trade 聚合单元测试"""

import unittest
from datetime import datetime, timezone

from app.aggregation.trades import TradeAggregator


class TestTradeAggregator(unittest.TestCase):
    def test_aggregate_empty(self):
        agg = TradeAggregator()
        count = agg.aggregate(
            inst_id="NONEXISTENT-SWAP",
            bar="1s",
            start=datetime(2020, 1, 1, tzinfo=timezone.utc),
            end=datetime(2020, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(count, 0)

    def test_aggregate_with_trades(self):
        """插入测试 trades 并验证聚合结果"""
        from app.database import get_engine
        from app.models import Trade
        from sqlalchemy import text
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        now = datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc)
        trades = [
            {
                "inst_id": "AGG-TEST-SWAP",
                "trade_id": f"agg-{i}",
                "ts": now + __import__("datetime").timedelta(seconds=i),
                "px": str(1000 + i),
                "sz": "1",
                "side": "buy" if i % 2 == 0 else "sell",
                "ingested_at": now,
                "raw_json": {},
            }
            for i in range(5)
        ]
        stmt = pg_insert(Trade).values(trades)
        stmt = stmt.on_conflict_do_nothing(index_elements=["inst_id", "trade_id", "ts"])
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(stmt)

        try:
            agg = TradeAggregator()
            count = agg.aggregate(
                inst_id="AGG-TEST-SWAP",
                bar="1s",
                start=now,
                end=now + __import__("datetime").timedelta(seconds=10),
            )
            self.assertGreater(count, 0)
        finally:
            # 清理
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM trades WHERE inst_id = 'AGG-TEST-SWAP'"))
                conn.execute(text("DELETE FROM trade_aggregates WHERE inst_id = 'AGG-TEST-SWAP'"))

    def test_model(self):
        from app.models import TradeAggregate
        self.assertEqual(TradeAggregate.__tablename__, "trade_aggregates")


if __name__ == "__main__":
    unittest.main()
