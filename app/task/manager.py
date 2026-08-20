"""TaskManager：供 FastAPI 使用的任务编排层（不碰子进程）

职责：
    - submit / batch：校验 TaskSpec → 建 job（PENDING→QUEUED）→ 审计
    - cancel：QUEUED/PAUSED 直接 CANCELLED；ASSIGNED/RUNNING 置 cancel_requested
    - pause / resume：仅 QUEUED / PAUSED
    - recover：把指定 worker 名下心跳过期的 ASSIGNED/RUNNING 置 INTERRUPTED
    - list / get / log 等查询透传
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from ..utils.logger import get_logger
from . import registry
from .store import JobStore, PostgresJobStore

logger = get_logger(__name__)


class TaskManager:
    """任务管理器（依赖注入 store，便于测试）"""

    AUDIT_CREATE = "CREATE_TASK"
    AUDIT_BATCH = "BATCH_CREATE_TASK"
    AUDIT_CANCEL = "CANCEL_TASK"
    AUDIT_PAUSE = "PAUSE_TASK"
    AUDIT_RESUME = "RESUME_TASK"
    AUDIT_REFRESH = "REFRESH_ASSETS"
    AUDIT_QUALITY = "QUALITY_CHECK"
    AUDIT_RETRY = "RETRY_TASK"

    def __init__(self, store: JobStore = None):
        self.store = store or PostgresJobStore()

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def submit(self, task_type: str, params: dict, group_id=None,
               parent_job_id=None) -> dict:
        """创建单个任务（校验 TaskSpec → PENDING → QUEUED）"""
        spec = registry.get_spec(task_type)
        if spec is None:
            raise ValueError(f"未知任务类型: {task_type}")
        validated = spec.validate(params)

        job_id = uuid.uuid4()
        job = {
            "id": job_id,
            "task_no": self.store.next_task_no(),
            "group_id": group_id,
            "task_type": task_type,
            "params": validated,
            "required_capability": spec.capability,
            "rate_group": spec.rate_group,
            "max_retry": validated.get("max_retry", 0),
            "priority": validated.get("priority", 0),
            "parent_job_id": parent_job_id,
            "status": "PENDING",
        }
        self.store.create_job(job)
        self.store.update_status(job_id, "PENDING", "QUEUED")
        self.store.add_audit(self.AUDIT_CREATE, "job", job_id, {
            "task_type": task_type,
            "task_no": job["task_no"],
        })
        logger.info("任务已创建: %s | %s | %s", task_type, job_id, validated)
        return self.store.get(job_id)

    def batch(self, task_type: str, params_list: List[dict],
              group_id: uuid.UUID = None) -> Dict:
        """批量创建（多交易对 N 任务，共享 group_id）→ {group_id, task_ids[]}"""
        group_id = group_id or uuid.uuid4()
        # Validate the whole request before writing anything.  This guarantees
        # validation failures never leave a partially-created batch behind.
        spec = registry.get_spec(task_type)
        if spec is None:
            raise ValueError(f"未知任务类型: {task_type}")
        validated_params = [spec.validate(params) for params in params_list]
        task_ids = []
        for params in validated_params:
            job = self.submit(task_type, params, group_id=group_id)
            task_ids.append(str(job["id"]))
        self.store.add_audit(self.AUDIT_BATCH, "job", group_id, {
            "task_type": task_type,
            "count": len(task_ids),
        })
        return {"group_id": str(group_id), "task_ids": task_ids}

    # ------------------------------------------------------------------
    # 取消 / 暂停 / 恢复
    # ------------------------------------------------------------------
    def cancel(self, job_id) -> dict:
        """取消任务：
        QUEUED/PAUSED → CANCELLED（直接）
        ASSIGNED/RUNNING → cancel_requested=true（由 Worker 终止子进程）
        """
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(f"任务不存在: {job_id}")
        status = job["status"]
        if status in ("ASSIGNED", "RUNNING"):
            self.store.update_status(
                job_id, status, status, cancel_requested=True
            )
            logger.info("任务取消请求已置位: %s (%s)", job_id, status)
        elif status in ("QUEUED", "PAUSED", "PENDING"):
            self.store.update_status(job_id, status, "CANCELLED")
            logger.info("任务已取消: %s", job_id)
        else:
            raise ValueError(f"终态任务不可取消: {status}")
        self.store.add_audit(self.AUDIT_CANCEL, "job", job_id, {"status": status})
        return self.store.get(job_id)

    def pause(self, job_id) -> dict:
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(f"任务不存在: {job_id}")
        if job["status"] != "QUEUED":
            raise ValueError(f"仅 QUEUED 任务可暂停: {job['status']}")
        self.store.update_status(job_id, "QUEUED", "PAUSED")
        self.store.add_audit(self.AUDIT_PAUSE, "job", job_id)
        return self.store.get(job_id)

    def resume(self, job_id) -> dict:
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(f"任务不存在: {job_id}")
        if job["status"] != "PAUSED":
            raise ValueError(f"仅 PAUSED 任务可恢复: {job['status']}")
        self.store.update_status(job_id, "PAUSED", "QUEUED")
        self.store.add_audit(self.AUDIT_RESUME, "job", job_id)
        return self.store.get(job_id)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get(self, job_id) -> Optional[dict]:
        return self.store.get(job_id)

    def list(self, status: str = None, limit: int = 50, offset: int = 0,
             task_type: str = None, group_id=None) -> Dict:
        return self.store.list(
            status=status, limit=limit, offset=offset,
            task_type=task_type, group_id=group_id,
        )

    def attempts(self, job_id) -> List[dict]:
        return self.store.list_attempts(job_id)

    def recover(self, worker_id, stale_seconds: int = 60) -> int:
        """重启 recovery：把指定 worker 名下心跳过期任务置 INTERRUPTED"""
        before = datetime.now(timezone.utc) - timedelta(seconds=stale_seconds)
        return self.store.mark_stale(worker_id, before)

    def audit(self, limit: int = 50, offset: int = 0) -> Dict:
        return self.store.list_audit(limit=limit, offset=offset)


def make_manager(store: JobStore = None) -> TaskManager:
    return TaskManager(store or PostgresJobStore())
