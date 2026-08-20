"""WorkerService：独立 Worker 进程的任务调度核心

    - 注册 workers 表（hostname/ip/python_version/os/capabilities）
    - 心跳（默认 10s）：更新 workers.last_heartbeat_at
    - 轮询 claim（默认 2s）：DB 原子领取 QUEUED 任务 → runner 子进程执行
    - 重启 recovery：启动时把本 worker 名下心跳过期的 ASSIGNED/RUNNING 置 INTERRUPTED
    - 退出：注册行置 OFFLINE
"""

import os
import platform
import signal
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from ..utils.logger import get_logger
from .runner import TaskRunner
from .store import JobStore, PostgresJobStore

logger = get_logger(__name__)

WORKER_VERSION = "1.0.0"


def _env_capabilities() -> List[str]:
    raw = os.getenv("WORKER_CAPABILITIES", "")
    if not raw:
        return ["download", "latency"]
    return [c.strip() for c in raw.split(",") if c.strip()]


class WorkerService:
    """单实例单并发（v1）；多开即多 Worker（DB 原子 claim 保证不重领）"""

    def __init__(
        self,
        store: JobStore = None,
        name: str = None,
        node: str = None,
        poll_interval: float = 2.0,
        heartbeat_interval: float = 10.0,
        capabilities: List[str] = None,
        concurrency: int = 1,
        runtime_root: str = "runtime/jobs",
    ):
        self.store = store or PostgresJobStore()
        self.name = name or os.getenv("WORKER_NAME") or socket.gethostname()
        self.node = node or os.getenv("WORKER_NODE") or self.name
        self.poll_interval = float(
            os.getenv("WORKER_POLL_INTERVAL", poll_interval)
        )
        self.heartbeat_interval = heartbeat_interval
        self.capabilities = capabilities or _env_capabilities()
        self.concurrency = max(1, concurrency)
        self.worker_id = uuid.uuid4()
        self.worker_record = None
        self.runner = TaskRunner(self.store, runtime_root=runtime_root)
        self._stop = threading.Event()
        self._busy = 0
        self._busy_lock = threading.Lock()
        self._running_job_ids = set()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._futures: set[Future] = set()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def register(self) -> dict:
        self.worker_record = self.store.register_worker({
            "id": self.worker_id,
            "name": self.name,
            "node": self.node,
            "hostname": socket.gethostname(),
            "ip": self._local_ip(),
            "python_version": platform.python_version(),
            "os": platform.platform(),
            "worker_version": WORKER_VERSION,
            "capabilities": self.capabilities,
            "capacity": self.concurrency,
        })
        logger.info(
            "Worker 注册完成: %s | node=%s | caps=%s | id=%s",
            self.name, self.node, self.capabilities, self.worker_id,
        )
        return self.worker_record

    def recover(self, stale_seconds: int = 90) -> int:
        """重启 recovery：本 worker 名下心跳过期的残留任务置 INTERRUPTED"""
        before = datetime.now(timezone.utc) - timedelta(seconds=stale_seconds)
        count = self.store.mark_stale(
            self.worker_id, before, name=self.name, node=self.node
        )
        if count:
            logger.warning("重启 recovery：%d 个残留任务置 INTERRUPTED", count)
        return count

    def start(self) -> None:
        self.register()
        self.recover()
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix=f"okx-worker-{self.name}",
        )
        # 信号处理：Ctrl+C / SIGTERM 优雅退出
        try:
            signal.signal(signal.SIGINT, self._on_signal)
            signal.signal(signal.SIGTERM, self._on_signal)
        except ValueError:
            pass  # 非主线程

        logger.info(
            "Worker 开始轮询: interval=%.1fs heartbeat=%.1fs",
            self.poll_interval, self.heartbeat_interval,
        )
        last_heartbeat = time.monotonic()
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if now - last_heartbeat >= self.heartbeat_interval:
                    self._do_heartbeat()
                    last_heartbeat = now
                self._reap_futures()
                if self._busy_count() < self.concurrency:
                    self._do_poll()
                self._stop.wait(self.poll_interval)
        finally:
            self._request_running_cancellation()
            if self._executor is not None:
                self._executor.shutdown(wait=True, cancel_futures=False)
            self._do_offline()

    def stop(self) -> None:
        self._stop.set()

    def _on_signal(self, signum, frame):
        logger.warning("收到信号 %s，Worker 优雅退出...", signum)
        self.stop()

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------
    def _local_ip(self) -> Optional[str]:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(2.0)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except OSError:
            return None

    def _do_heartbeat(self) -> None:
        try:
            self.store.worker_heartbeat(self.worker_id)
            self.store.touch_worker_status(
                self.worker_id,
                "BUSY" if self._busy_count() > 0 else "IDLE",
                current_task_count=self._busy_count(),
            )
        except Exception as e:
            logger.warning("Worker 心跳失败: %s", e)

    def _do_poll(self) -> None:
        job = None
        try:
            job = self.store.claim_next(self.worker_id, self.capabilities)
        except Exception as e:
            logger.warning("认领任务失败: %s", e)
            self.store.touch_worker_status(
                self.worker_id, "ERROR", last_error=str(e)[:2000]
            )
            return
        if job is None:
            return
        with self._busy_lock:
            self._busy += 1
            self._running_job_ids.add(str(job["id"]))
        if self._executor is None:
            raise RuntimeError("Worker executor 未初始化")
        future = self._executor.submit(self._run_job, job)
        self._futures.add(future)

    def _run_job(self, job: dict) -> None:
        try:
            self.runner.run(job, self.worker_id)
        except Exception as e:
            logger.error("任务执行异常: %s | %s", job["id"], e)
            # 兜底：意外异常时确保任务不永久停留在 RUNNING/ASSIGNED
            try:
                current = self.store.get(job["id"])
                if current and current["status"] in ("ASSIGNED", "RUNNING"):
                    self.store.update_status(
                        job["id"], current["status"], "FAILED",
                        error=str(e)[:2000],
                    )
            except Exception:
                pass
        finally:
            with self._busy_lock:
                self._busy -= 1
                self._running_job_ids.discard(str(job["id"]))

    def _busy_count(self) -> int:
        with self._busy_lock:
            return self._busy

    def _reap_futures(self) -> None:
        done = {future for future in self._futures if future.done()}
        self._futures.difference_update(done)
        for future in done:
            try:
                future.result()
            except Exception as exc:
                logger.error("Worker 执行线程异常: %s", exc)

    def _request_running_cancellation(self) -> None:
        """Tell task runners to stop before waiting for their child processes."""
        with self._busy_lock:
            job_ids = list(self._running_job_ids)
        for job_id in job_ids:
            try:
                job = self.store.get(job_id)
                if job and job.get("status") in ("ASSIGNED", "RUNNING"):
                    self.store.update_status(
                        job_id, job["status"], job["status"],
                        cancel_requested=True,
                    )
            except Exception as exc:
                logger.warning("Worker 退出时请求取消失败: %s", exc)

    def _do_offline(self) -> None:
        try:
            self.store.touch_worker_status(self.worker_id, "OFFLINE")
            logger.info("Worker 已置 OFFLINE")
        except Exception as e:
            logger.warning("Worker 下线标记失败: %s", e)


def main() -> int:
    from ..db.database import init_db

    init_db()
    service = WorkerService()
    service.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
