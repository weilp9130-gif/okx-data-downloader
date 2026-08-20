"""Web API 测试：TestClient + InMemory store override"""

import json
import unittest

from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.task.store import InMemoryJobStore
from app.web.main import create_app


class TestWebAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = InMemoryJobStore()
        cls.app = create_app(store=cls.store, seed_schema=False)
        cls.client = TestClient(cls.app)

    def _submit_kline(self, **kw):
        params = {
            "inst": "BTC-USDT-SWAP",
            "bars": ["1D"],
            "start": "2024-01-01",
            "end": "2024-02-01",
        }
        params.update(kw)
        return self.client.post("/api/tasks", json={"task_type": "KLINE", "params": params})

    def test_health(self):
        r = self.client.get("/api/health")
        self.assertEqual(r.status_code, 200)
        self.assertIn("status", r.json())
        self.assertIn("db", r.json())

    def test_system_info_masks_secrets(self):
        r = self.client.get("/api/system/info")
        self.assertEqual(r.status_code, 200)
        info = r.json()
        self.assertIn("version", info)
        masked = info["masked_keys"]
        for key in ("OKX_SECRET_KEY", "OKX_PASSPHRASE"):
            self.assertIn(key, masked)
            value = masked[key]
            if value:
                self.assertNotIn("passphrase-value", value)

    def test_system_logs(self):
        r = self.client.get("/api/system/logs")
        self.assertEqual(r.status_code, 200)
        self.assertIn("files", r.json())

    def test_bars(self):
        r = self.client.get("/api/bars")
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(len(r.json()["items"]), 13)

    def test_create_task(self):
        r = self._submit_kline()
        self.assertEqual(r.status_code, 200)
        job = r.json()
        self.assertEqual(job["status"], "QUEUED")
        self.assertEqual(job["task_type"], "KLINE")
        self.assertTrue(job["task_no"].startswith("TASK-"))

    def test_create_task_invalid_params_422(self):
        r = self.client.post("/api/tasks", json={
            "task_type": "KLINE",
            "params": {"inst": "BTC-USDT-SWAP", "bars": ["99Z"],
                       "start": "2024-01-01", "end": "2024-02-01"},
        })
        self.assertEqual(r.status_code, 422)

    def test_create_task_extra_field_422(self):
        r = self.client.post("/api/tasks", json={
            "task_type": "KLINE",
            "params": {"inst": "BTC-USDT-SWAP", "bars": ["1D"], "evil": 1,
                       "start": "2024-01-01", "end": "2024-02-01"},
        })
        self.assertEqual(r.status_code, 422)

    def test_batch(self):
        r = self.client.post("/api/tasks/batch", json={
            "task_type": "KLINE",
            "params_list": [
                {"inst": "BTC-USDT-SWAP", "bars": ["1D"],
                 "start": "2024-01-01", "end": "2024-02-01"},
                {"inst": "ETH-USDT-SWAP", "bars": ["1D"],
                 "start": "2024-01-01", "end": "2024-02-01"},
            ],
        })
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(len(data["task_ids"]), 2)
        self.assertIsNotNone(data["group_id"])

    def test_list_tasks(self):
        self._submit_kline()
        r = self.client.get("/api/tasks")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertGreaterEqual(body["total"], 1)
        self.assertIn("items", body)

    def test_task_detail_and_attempts(self):
        job = self._submit_kline().json()
        r = self.client.get(f"/api/tasks/{job['id']}")
        self.assertEqual(r.status_code, 200)
        self.assertIn("attempts", r.json())
        r2 = self.client.get(f"/api/tasks/{job['id']}/attempts")
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.json(), [])

    def test_stop_queued(self):
        job = self._submit_kline().json()
        r = self.client.post(f"/api/tasks/{job['id']}/stop")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "CANCELLED")

    def test_pause_resume(self):
        job = self._submit_kline().json()
        r = self.client.post(f"/api/tasks/{job['id']}/pause")
        self.assertEqual(r.json()["status"], "PAUSED")
        r = self.client.post(f"/api/tasks/{job['id']}/resume")
        self.assertEqual(r.json()["status"], "QUEUED")

    def test_log_empty(self):
        job = self._submit_kline().json()
        r = self.client.get(f"/api/tasks/{job['id']}/log")
        # 无 attempt 时 404 或空日志均可（未执行）
        self.assertIn(r.status_code, (200, 404))

    def test_not_found(self):
        r = self.client.get("/api/tasks/does-not-exist")
        self.assertEqual(r.status_code, 404)

    def test_audit(self):
        self._submit_kline()
        r = self.client.get("/api/audit")
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["total"], 1)

    def test_quality_check_task(self):
        r = self.client.post("/api/quality/check", json={
            "inst_id": "BTC-USDT-SWAP", "bar": "1D", "cross_source": False,
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["task_type"], "QUALITY_CHECK")

    def test_asset_refresh_task(self):
        r = self.client.post("/api/assets/refresh", json={
            "scope": "all", "mode": "incremental",
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["task_type"], "ASSET_REFRESH")


if __name__ == "__main__":
    unittest.main()
