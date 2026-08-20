"""Trade 聚合单元测试"""

import unittest
import pytest
from datetime import datetime, timezone

from app.aggregation.trades import TradeAggregator


class TestOrderBookFactorCalculator(unittest.TestCase):
    def test_calculate(self):
        from app.aggregation.orderbook import OrderBookFactorCalculator
        bids = [["63000", "10"], ["62990", "5"]]
        asks = [["63001", "8"], ["63002", "3"]]
        factors = OrderBookFactorCalculator.calculate(bids, asks)
        self.assertIsNotNone(factors)
        self.assertGreater(factors["spread"], 0)
        self.assertAlmostEqual(factors["mid"], 63000.5)
        self.assertEqual(factors["bid_depth_5"], 15.0)
        self.assertEqual(factors["ask_depth_5"], 11.0)
        self.assertIn("imbalance_5", factors)

    def test_calculate_empty(self):
        from app.aggregation.orderbook import OrderBookFactorCalculator
        self.assertIsNone(OrderBookFactorCalculator.calculate([], []))


@pytest.mark.integration
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
        from app.db.database import get_engine
        from app.db.models import Trade
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

    def test_bucket_boundary_fractional_seconds(self):
        """回归：EXTRACT(EPOCH)::bigint 会四舍五入导致桶错位（floor 修复）

        ts=00:00:00.900 的成交必须属于 00:00 桶而非 00:01 桶。
        """
        from app.db.database import get_engine
        from app.db.models import Trade
        from sqlalchemy import text
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        base = datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc)
        trades = [
            {
                "inst_id": "AGG-BND-SWAP",
                "trade_id": "b-1",
                "ts": base.replace(microsecond=100000),  # 10:00:00.100
                "px": "100",
                "sz": "1",
                "side": "buy",
                "ingested_at": base,
                "raw_json": {},
            },
            {
                "inst_id": "AGG-BND-SWAP",
                "trade_id": "b-2",
                "ts": base.replace(microsecond=900000),  # 10:00:00.900
                "px": "101",
                "sz": "1",
                "side": "buy",
                "ingested_at": base,
                "raw_json": {},
            },
            {
                "inst_id": "AGG-BND-SWAP",
                "trade_id": "b-3",
                "ts": base.replace(second=1, microsecond=100000),  # 10:00:01.100
                "px": "102",
                "sz": "1",
                "side": "buy",
                "ingested_at": base,
                "raw_json": {},
            },
        ]
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                pg_insert(Trade).values(trades).on_conflict_do_nothing(
                    index_elements=["inst_id", "trade_id", "ts"]
                )
            )

        try:
            agg = TradeAggregator()
            agg.aggregate(
                inst_id="AGG-BND-SWAP",
                bar="1s",
                start=base,
                end=base + __import__("datetime").timedelta(seconds=5),
            )
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT ts, o, c, cnt FROM trade_aggregates
                        WHERE inst_id = 'AGG-BND-SWAP' ORDER BY ts
                        """
                    )
                ).fetchall()
                # 2 个桶：10:00:00（含 0.1 与 0.9 两笔）、10:00:01（一笔）
                self.assertEqual(len(rows), 2)
                first, second = rows
                self.assertEqual(float(first[1]), 100.0)   # o=首笔 100
                self.assertEqual(float(first[2]), 101.0)   # c=末笔 101
                self.assertEqual(int(first[3]), 2)         # cnt=2
                self.assertEqual(float(second[1]), 102.0)
                self.assertEqual(int(second[3]), 1)
        finally:
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM trades WHERE inst_id = 'AGG-BND-SWAP'"))
                conn.execute(text("DELETE FROM trade_aggregates WHERE inst_id = 'AGG-BND-SWAP'"))

    def test_model(self):
        from app.db.models import TradeAggregate
        self.assertEqual(TradeAggregate.__tablename__, "trade_aggregates")


if __name__ == "__main__":
    unittest.main()
