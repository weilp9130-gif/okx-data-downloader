"""Phase 8 Recovery 单元测试"""

import unittest
from datetime import datetime, timezone

from app.realtime.recovery import DataGapStore, RecoveryEventStore


class TestRecoveryEventStore(unittest.TestCase):
    def test_start_finish(self):
        store = RecoveryEventStore()
        now = datetime.now(timezone.utc)
        rid = store.start("trades", "BTC-USDT-SWAP", reason="WS_DISCONNECT",
                          from_ts=now, to_ts=now)
        self.assertTrue(rid)
        store.finish(rid, "RECOVERED", 100)
        with store.engine.connect() as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT status, rows_recovered FROM recovery_events WHERE recovery_id = :rid"),
                {"rid": rid},
            ).mappings().one()
            self.assertEqual(row["status"], "RECOVERED")
            self.assertEqual(row["rows_recovered"], 100)


class TestDataGapStore(unittest.TestCase):
    def test_register_dedup(self):
        store = DataGapStore()
        now = datetime.now(timezone.utc)
        start = now
        end = now.replace(second=30)
        # 第一次登记
        first = store.register_open("trades", "BTC-USDT-SWAP", start, end, "WS_DISCONNECT")
        self.assertTrue(first)
        # 重复登记应跳过
        second = store.register_open("trades", "BTC-USDT-SWAP", start, end, "WS_DISCONNECT")
        self.assertFalse(second)
        # 标记恢复
        store.mark_recovered("trades", "BTC-USDT-SWAP", start, end, 50)
        with store.engine.connect() as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT status, recovery_rows FROM data_gaps
                    WHERE data_type = 'trades' AND inst_id = 'BTC-USDT-SWAP'
                    ORDER BY id DESC LIMIT 1
                    """
                ),
            ).mappings().one()
            self.assertEqual(row["status"], "RECOVERED")
            self.assertEqual(row["recovery_rows"], 50)

    def test_mark_unrecoverable(self):
        store = DataGapStore()
        now = datetime.now(timezone.utc)
        start = now
        end = now.replace(second=45)
        store.register_open("mark", "BTC-USDT-SWAP", start, end, "SEQ_GAP")
        store.mark_unrecoverable("mark", "BTC-USDT-SWAP", start, end, "API range exceeded")
        with store.engine.connect() as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT status FROM data_gaps
                    WHERE data_type = 'mark' AND inst_id = 'BTC-USDT-SWAP'
                    ORDER BY id DESC LIMIT 1
                    """
                ),
            ).mappings().one()
            self.assertEqual(row["status"], "UNRECOVERABLE")


class TestRecoveryModels(unittest.TestCase):
    def test_models(self):
        from app.db.models import DataGap, RecoveryEvent
        self.assertEqual(RecoveryEvent.__tablename__, "recovery_events")
        self.assertEqual(DataGap.__tablename__, "data_gaps")


if __name__ == "__main__":
    unittest.main()
