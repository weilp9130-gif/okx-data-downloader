"""TaskManager 测试：InMemoryJobStore 上 submit/cancel/pause/resume/stale"""

import unittest
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError

from app.task.manager import TaskManager
from app.task.store import InMemoryJobStore


def _kline_params(**kw):
    p = {
        "inst": "BTC-USDT-SWAP",
        "bars": ["1D"],
        "start": "2024-01-01",
        "end": "2024-02-01",
    }
    p.update(kw)
    return p


class TestManagerSubmit(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryJobStore()
        self.mgr = TaskManager(self.store)

    def test_submit_creates_queued_job(self):
        job = self.mgr.submit("KLINE", _kline_params())
        self.assertEqual(job["status"], "QUEUED")
        self.assertTrue(job["task_no"].startswith("TASK-"))
        self.assertEqual(job["required_capability"], "download")
        self.assertEqual(job["rate_group"], "okx_market")
        self.assertEqual(job["max_retry"], 0)
        self.assertEqual(job["attempt_no"], 0)

    def test_submit_unknown_type(self):
        with self.assertRaises(ValueError):
            self.mgr.submit("NOPE", {})

    def test_submit_invalid_params(self):
        with self.assertRaises(ValidationError):
            self.mgr.submit("KLINE", _kline_params(bars=["99Z"]))

    def test_submit_audit(self):
        self.mgr.submit("KLINE", _kline_params())
        audits = self.mgr.audit()["items"]
        self.assertEqual(audits[0]["action"], "CREATE_TASK")

    def test_task_no_sequential(self):
        j1 = self.mgr.submit("KLINE", _kline_params())
        j2 = self.mgr.submit("KLINE", _kline_params())
        self.assertEqual(j1["task_no"][-5:], "00001")
        self.assertEqual(j2["task_no"][-5:], "00002")


class TestManagerCancelPauseResume(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryJobStore()
        self.mgr = TaskManager(self.store)

    def test_cancel_queued(self):
        job = self.mgr.submit("KLINE", _kline_params())
        after = self.mgr.cancel(job["id"])
        self.assertEqual(after["status"], "CANCELLED")
        self.assertEqual(after["finished_at"] is not None, True)

    def test_cancel_running_sets_request(self):
        job = self.mgr.submit("KLINE", _kline_params())
        claimed = self.store.claim_next("w1", ["download"])
        self.store.update_status(job["id"], "ASSIGNED", "RUNNING", pid=123)
        after = self.mgr.cancel(job["id"])
        self.assertEqual(after["status"], "RUNNING")
        self.assertTrue(after["cancel_requested"])

    def test_cancel_terminal_rejected(self):
        job = self.mgr.submit("KLINE", _kline_params())
        self.store.update_status(job["id"], "QUEUED", "CANCELLED")
        with self.assertRaises(ValueError):
            self.mgr.cancel(job["id"])

    def test_pause_resume(self):
        job = self.mgr.submit("KLINE", _kline_params())
        paused = self.mgr.pause(job["id"])
        self.assertEqual(paused["status"], "PAUSED")
        resumed = self.mgr.resume(job["id"])
        self.assertEqual(resumed["status"], "QUEUED")

    def test_pause_only_queued(self):
        job = self.mgr.submit("KLINE", _kline_params())
        self.store.update_status(job["id"], "QUEUED", "RUNNING")
        with self.assertRaises(ValueError):
            self.mgr.pause(job["id"])

    def test_resume_only_paused(self):
        job = self.mgr.submit("KLINE", _kline_params())
        with self.assertRaises(ValueError):
            self.mgr.resume(job["id"])

    def test_paused_cancel(self):
        job = self.mgr.submit("KLINE", _kline_params())
        self.mgr.pause(job["id"])
        after = self.mgr.cancel(job["id"])
        self.assertEqual(after["status"], "CANCELLED")


class TestManagerBatch(unittest.TestCase):
    def test_batch_shared_group_id(self):
        store = InMemoryJobStore()
        mgr = TaskManager(store)
        result = mgr.batch("KLINE", [_kline_params(), _kline_params()])
        self.assertIsNotNone(result["group_id"])
        self.assertEqual(len(result["task_ids"]), 2)
        jobs = store.list()["items"]
        self.assertEqual(len(jobs), 2)
        for j in jobs:
            self.assertEqual(str(j["group_id"]), result["group_id"])

    def test_batch_invalid_one_fails_all(self):
        store = InMemoryJobStore()
        mgr = TaskManager(store)
        with self.assertRaises(ValidationError):
            mgr.batch("KLINE", [_kline_params(), _kline_params(bars=["nope"])])
        self.assertEqual(store.list()["total"], 0)


class TestManagerRecover(unittest.TestCase):
    def test_recover_marks_stale_interrupted(self):
        store = InMemoryJobStore()
        mgr = TaskManager(store)
        job = mgr.submit("KLINE", _kline_params())
        claimed = store.claim_next("w1", ["download"])
        # 心跳过期
        self.store_get(store, job["id"])["heartbeat_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        n = mgr.recover("w1", stale_seconds=60)
        self.assertEqual(n, 1)
        self.assertEqual(store.get(job["id"])["status"], "INTERRUPTED")

    def test_recover_fresh_not_touched(self):
        store = InMemoryJobStore()
        mgr = TaskManager(store)
        job = mgr.submit("KLINE", _kline_params())
        store.claim_next("w1", ["download"])
        # 刚心跳过（新鲜）
        self.store_get(store, job["id"])["heartbeat_at"] = datetime.now(timezone.utc)
        n = mgr.recover("w1", stale_seconds=60)
        self.assertEqual(n, 0)

    @staticmethod
    def store_get(store, job_id):
        return store._jobs[str(job_id)]


if __name__ == "__main__":
    unittest.main()
