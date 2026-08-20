"""WorkerService 测试：InMemory + fake_task
claim→RUNNING→SUCCESS/FAILED/attempt 记录/cancel_requested/recovery
"""

import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict

from app.task import registry
from app.task.manager import TaskManager
from app.task.registry import TaskSpec
from app.task.store import InMemoryJobStore
from app.task.worker_service import WorkerService


class FakeParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    exit_code: int = 0
    sleep: float = 0.2
    max_retry: int = 0
    priority: int = 0


def _build_fake(argv_params, progress_file):
    return [
        sys.executable, "-m", "tests.fixtures.fake_task",
        "--progress-file", progress_file,
        "--exit", str(argv_params["exit_code"]),
        "--sleep", str(argv_params["sleep"]),
    ]


registry.REGISTRY["FAKE"] = TaskSpec(
    "FAKE", FakeParams, capability="download", rate_group=None,
    build_argv=_build_fake,
)


def _wait_status(store, job_id, wanted, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = store.get(job_id)
        if job and job["status"] in wanted:
            return job
        time.sleep(0.05)
    raise AssertionError(f"等待状态 {wanted} 超时: {store.get(job_id)}")


class TestWorkerService(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryJobStore()
        self.mgr = TaskManager(self.store)

    def _start_worker(self):
        service = WorkerService(
            store=self.store,
            name="test-worker",
            node="test-node",
            poll_interval=0.05,
            heartbeat_interval=0.1,
            capabilities=["download"],
        )
        t = threading.Thread(target=service.start, daemon=True)
        t.start()
        return service

    def _start_parallel_worker(self):
        service = WorkerService(
            store=self.store, name="parallel-worker", node="test-node",
            poll_interval=0.02, heartbeat_interval=0.1,
            capabilities=["download"], concurrency=2,
        )
        t = threading.Thread(target=service.start, daemon=True)
        t.start()
        return service

    def test_claim_to_success(self):
        service = self._start_worker()
        try:
            job = self.mgr.submit("FAKE", {"exit_code": 0, "sleep": 0.2})
            done = _wait_status(self.store, job["id"], {"SUCCESS", "FAILED"})
            self.assertEqual(done["status"], "SUCCESS")
            self.assertEqual(done["exit_code"], 0)
            self.assertEqual(done["attempt_no"], 1)
            self.assertIsNotNone(done["assigned_worker_id"])
            attempts = self.store.list_attempts(job["id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["exit_code"], 0)
            self.assertIsNotNone(attempts[0]["log_path"])
            # progress 已写 DB
            self.assertIsNotNone(done["progress"])
        finally:
            service.stop()

    def test_claim_to_failed(self):
        service = self._start_worker()
        try:
            job = self.mgr.submit("FAKE", {"exit_code": 1, "sleep": 0.1})
            done = _wait_status(self.store, job["id"], {"SUCCESS", "FAILED"})
            self.assertEqual(done["status"], "FAILED")
            self.assertEqual(done["exit_code"], 1)
        finally:
            service.stop()

    def test_cancel_running(self):
        service = self._start_worker()
        try:
            job = self.mgr.submit("FAKE", {"exit_code": 0, "sleep": 5.0})
            running = _wait_status(self.store, job["id"], {"RUNNING"})
            self.assertEqual(running["status"], "RUNNING")
            self.mgr.cancel(job["id"])
            done = _wait_status(self.store, job["id"], {"CANCELLED"})
            self.assertEqual(done["status"], "CANCELLED")
            self.assertTrue(done["cancel_requested"])
        finally:
            service.stop()

    def test_retry_when_max_retry(self):
        service = self._start_worker()
        try:
            job = self.mgr.submit("FAKE", {
                "exit_code": 1, "sleep": 0.05, "max_retry": 1,
            })
            done = _wait_status(self.store, job["id"],
                                {"SUCCESS", "FAILED"}, timeout=20)
            # 两次 attempt 都失败后终态 FAILED
            self.assertEqual(done["status"], "FAILED")
            self.assertEqual(done["retry_count"], 1)
            attempts = self.store.list_attempts(job["id"])
            self.assertGreaterEqual(len(attempts), 1)
        finally:
            service.stop()

    def test_recovery_marks_stale(self):
        service = WorkerService(
            store=self.store, name="r", node="r",
            capabilities=["download"],
        )
        job = self.mgr.submit("FAKE", {"exit_code": 0, "sleep": 0.1})
        self.store.claim_next(service.worker_id, ["download"])
        # 心跳过期
        self.store._jobs[str(job["id"])]["heartbeat_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        n = service.recover(stale_seconds=60)
        self.assertEqual(n, 1)
        self.assertEqual(self.store.get(job["id"])["status"], "INTERRUPTED")

    def test_register_records_worker(self):
        service = WorkerService(
            store=self.store, name="w1", node="n1",
            capabilities=["download"],
        )
        record = service.register()
        self.assertEqual(record["name"], "w1")
        self.assertEqual(record["status"], "IDLE")
        self.assertIn("download", record["capabilities"])
        workers = self.store.list_workers()
        self.assertEqual(len(workers), 1)

    def test_concurrency_claims_two_jobs(self):
        service = self._start_parallel_worker()
        try:
            first = self.mgr.submit("FAKE", {"sleep": 0.7})
            second = self.mgr.submit("FAKE", {"sleep": 0.7})
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                running = [self.store.get(j["id"])["status"] for j in (first, second)]
                if running.count("RUNNING") == 2:
                    break
                time.sleep(0.03)
            self.assertEqual(running.count("RUNNING"), 2)
        finally:
            service.stop()


if __name__ == "__main__":
    unittest.main()
