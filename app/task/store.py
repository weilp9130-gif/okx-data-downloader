"""任务存储：JobStore 协议 + Postgres 实现 + InMemory 测试实现

Worker 与 FastAPI 只通过 DB 通信：
    - submit/create_job     → 写入 jobs=QUEUED
    - claim_next            → DB 原子领取（UPDATE ... WHERE id IN (SELECT ...)）
    - update_status         → 带 from 条件的状态流转（并发安全）
    - progress/heartbeat    → 运行期增量写入
    - attempts              → job_attempts 记录（日志/进度按 attempt 分文件）
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import text

from ..db.database import session_scope
from ..db.models import JobAttempt, TaskJob, METADATA_SCHEMA
from .state_machine import TERMINAL_STATES

SCHEMA = METADATA_SCHEMA


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class IllegalTransitionError(Exception):
    pass


class JobStore:
    """JobStore 协议（子类必须实现全部方法）"""

    def create_job(self, job: dict) -> dict:
        raise NotImplementedError

    def claim_next(self, worker_id, capabilities: List[str]) -> Optional[dict]:
        raise NotImplementedError

    def update_status(
        self, job_id, from_state: str, to_state: str, retry: bool = False, **fields
    ) -> dict:
        raise NotImplementedError

    def append_progress(self, job_id, event: dict) -> None:
        raise NotImplementedError

    def heartbeat(self, job_id) -> None:
        raise NotImplementedError

    def worker_heartbeat(self, worker_id) -> None:
        raise NotImplementedError

    def mark_stale(self, worker_id, heartbeat_before: datetime) -> int:
        raise NotImplementedError

    def create_attempt(self, job_id, attempt: dict) -> int:
        raise NotImplementedError

    def finish_attempt(self, attempt_id: int, **fields) -> None:
        raise NotImplementedError

    def get(self, job_id) -> Optional[dict]:
        raise NotImplementedError

    def list(self, status: Optional[str] = None, limit: int = 50, offset: int = 0,
             task_type: Optional[str] = None, group_id=None) -> Dict:
        raise NotImplementedError

    def list_attempts(self, job_id) -> List[dict]:
        raise NotImplementedError

    def touch_worker_status(self, worker_id, status: str, **fields) -> None:
        raise NotImplementedError

    def count(self, status: str) -> int:
        raise NotImplementedError

    def next_task_no(self) -> str:
        raise NotImplementedError

    def add_audit(self, action: str, target_type: str = None,
                  target_id=None, detail: dict = None) -> None:
        raise NotImplementedError

    def list_audit(self, limit: int = 50, offset: int = 0) -> Dict:
        raise NotImplementedError

    def register_worker(self, worker: dict) -> dict:
        raise NotImplementedError

    def get_worker(self, worker_id) -> Optional[dict]:
        raise NotImplementedError

    def list_workers(self) -> List[dict]:
        raise NotImplementedError


class PostgresJobStore(JobStore):
    """PostgreSQL 实现（SQLAlchemy Core 直连，保证原子领取）"""

    def create_job(self, job: dict) -> dict:
        job_id = job.get("id") or uuid.uuid4()
        now = utcnow()
        row = {
            "id": job_id,
            "group_id": job.get("group_id"),
            "task_no": job["task_no"],
            "task_type": job["task_type"],
            "params": job.get("params", {}),
            "status": job.get("status", "QUEUED"),
            "priority": job.get("priority", 0),
            "required_capability": job.get("required_capability"),
            "rate_group": job.get("rate_group"),
            "attempt_no": 0,
            "retry_count": job.get("retry_count", 0),
            "max_retry": job.get("max_retry", 0),
            "parent_job_id": job.get("parent_job_id"),
            "depends_on_job_id": job.get("depends_on_job_id"),
            "workflow_id": job.get("workflow_id"),
            "cancel_requested": bool(job.get("cancel_requested", False)),
            "created_at": now,
            "updated_at": now,
        }
        with session_scope() as s:
            s.add(TaskJob(**row))
        return self.get(job_id)

    def claim_next(self, worker_id, capabilities: List[str]) -> Optional[dict]:
        """DB 原子领取下一个 QUEUED 任务（多 Worker 不重复领取）"""
        if not capabilities:
            return None
        sql = f"""
            UPDATE {SCHEMA}.jobs
            SET status = 'ASSIGNED',
                assigned_worker_id = :worker_id,
                node = :node,
                attempt_no = attempt_no + 1,
                started_at = :now,
                updated_at = :now
            WHERE id = (
                SELECT id FROM {SCHEMA}.jobs
                WHERE status = 'QUEUED'
                  AND (required_capability IS NULL
                       OR required_capability IN :caps)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        """
        from sqlalchemy import bindparam

        stmt = text(sql).bindparams(bindparam("caps", expanding=True))
        with session_scope() as s:
            result = s.execute(
                stmt,
                {
                    "worker_id": worker_id,
                    "node": None,
                    "now": utcnow(),
                    "caps": capabilities,
                },
            )
            rows = result.fetchall()
            if not rows:
                return None
            job_id = rows[0][0]
        return self.get(job_id)

    def update_status(
        self, job_id, from_state: str, to_state: str, retry: bool = False, **fields
    ) -> dict:
        sets = ["status = :to", "updated_at = :now"]
        params = {
            "id": job_id,
            "from": from_state,
            "to": to_state,
            "now": utcnow(),
        }
        if to_state in TERMINAL_STATES:
            sets.append("finished_at = :now")
        if to_state == "QUEUED" and retry:
            sets.append("retry_count = retry_count + 1")
            sets.append("started_at = NULL")
            sets.append("finished_at = NULL")
        for k, v in fields.items():
            if v is not None:
                sets.append(f"{k} = :{k}")
                params[k] = v
        sql = (
            f"UPDATE {SCHEMA}.jobs SET {', '.join(sets)} "
            f"WHERE id = :id AND status = :from"
        )
        with session_scope() as s:
            result = s.execute(text(sql), params)
            if result.rowcount == 0:
                raise IllegalTransitionError(
                    f"{from_state} → {to_state} 已被并发状态变更阻止"
                )
        return self.get(job_id)

    def append_progress(self, job_id, event: dict) -> None:
        import json as _json

        with session_scope() as s:
            s.execute(
                text(
                    f"UPDATE {SCHEMA}.jobs SET progress = CAST(:p AS jsonb), "
                    f"updated_at = :now WHERE id = :id"
                ),
                {"p": _json.dumps(event, ensure_ascii=False, default=str),
                 "id": job_id, "now": utcnow()},
            )

    def heartbeat(self, job_id) -> None:
        with session_scope() as s:
            s.execute(
                text(
                    f"UPDATE {SCHEMA}.jobs SET heartbeat_at = :now, updated_at = :now "
                    f"WHERE id = :id AND status IN ('ASSIGNED', 'RUNNING')"
                ),
                {"id": job_id, "now": utcnow()},
            )

    def worker_heartbeat(self, worker_id) -> None:
        with session_scope() as s:
            s.execute(
                text(
                    f"UPDATE {SCHEMA}.workers SET last_heartbeat_at = :now, "
                    f"updated_at = :now WHERE id = :id"
                ),
                {"id": worker_id, "now": utcnow()},
            )

    def mark_stale(self, worker_id, heartbeat_before: datetime,
                   name: str = None, node: str = None) -> int:
        """把本 worker 名下心跳过期的 ASSIGNED/RUNNING 置 INTERRUPTED

        按 assigned_worker_id 或 (name, node) 匹配——Worker 每次重启 UUID
        会变，但 name/node 稳定，保证重启后能接管上一实例的残留任务。
        """
        extra = ""
        params = {
            "worker_id": worker_id,
            "before": heartbeat_before,
            "now": utcnow(),
        }
        if name:
            extra += (
                " OR assigned_worker_id IN ("
                "SELECT id FROM metadata.workers WHERE name = :name"
            )
            params["name"] = name
            if node:
                extra += " AND node = :node"
                params["node"] = node
            extra += ")"
        sql = (
            f"UPDATE {SCHEMA}.jobs SET status = 'INTERRUPTED', finished_at = :now, "
            f"updated_at = :now "
            f"WHERE status IN ('ASSIGNED', 'RUNNING') "
            f"AND assigned_worker_id = :worker_id "
            f"AND (heartbeat_at IS NULL OR heartbeat_at < :before)"
            + extra
        )
        with session_scope() as s:
            result = s.execute(text(sql), params)
            return result.rowcount or 0

    def create_attempt(self, job_id, attempt: dict) -> int:
        with session_scope() as s:
            a = JobAttempt(
                job_id=job_id,
                attempt_no=attempt["attempt_no"],
                worker_id=attempt.get("worker_id"),
                pid=attempt.get("pid"),
                started_at=attempt.get("started_at") or utcnow(),
                log_path=attempt.get("log_path"),
                progress_path=attempt.get("progress_path"),
            )
            s.add(a)
            s.flush()
            return a.id

    def finish_attempt(self, attempt_id: int, **fields) -> None:
        sets = []
        params = {"id": attempt_id}
        for k, v in fields.items():
            sets.append(f"{k} = :{k}")
            params[k] = v
        if sets:
            with session_scope() as s:
                s.execute(
                    text(f"UPDATE {SCHEMA}.job_attempts SET {', '.join(sets)} WHERE id = :id"),
                    params,
                )

    def get(self, job_id) -> Optional[dict]:
        with session_scope() as s:
            row = (
                s.query(TaskJob)
                .filter(TaskJob.id == job_id)
                .first()
            )
            if row is None:
                return None
            return _job_to_dict(row)

    def list(self, status: Optional[str] = None, limit: int = 50, offset: int = 0,
             task_type: Optional[str] = None, group_id=None) -> Dict:
        with session_scope() as s:
            q = s.query(TaskJob)
            if status:
                q = q.filter(TaskJob.status == status)
            if task_type:
                q = q.filter(TaskJob.task_type == task_type)
            if group_id is not None:
                q = q.filter(TaskJob.group_id == group_id)
            total = q.count()
            rows = (
                q.order_by(TaskJob.created_at.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )
            return {
                "total": total,
                "items": [_job_to_dict(r) for r in rows],
            }

    def list_attempts(self, job_id) -> List[dict]:
        with session_scope() as s:
            rows = (
                s.query(JobAttempt)
                .filter(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_no.asc())
                .all()
            )
            return [
                {
                    "id": r.id,
                    "attempt_no": r.attempt_no,
                    "worker_id": r.worker_id,
                    "pid": r.pid,
                    "started_at": r.started_at,
                    "finished_at": r.finished_at,
                    "exit_code": r.exit_code,
                    "log_path": r.log_path,
                    "progress_path": r.progress_path,
                    "error": r.error,
                }
                for r in rows
            ]

    def touch_worker_status(self, worker_id, status: str, **fields) -> None:
        sets = ["status = :status", "updated_at = :now"]
        params = {"id": worker_id, "status": status, "now": utcnow()}
        for k, v in fields.items():
            sets.append(f"{k} = :{k}")
            params[k] = v
        with session_scope() as s:
            s.execute(
                text(f"UPDATE {SCHEMA}.workers SET {', '.join(sets)} WHERE id = :id"),
                params,
            )

    def count(self, status: str) -> int:
        with session_scope() as s:
            return (
                s.query(TaskJob)
                .filter(TaskJob.status == status)
                .count()
            )

    def next_task_no(self) -> str:
        with session_scope() as s:
            today = utcnow().date()
            count = (
                s.query(TaskJob)
                .filter(TaskJob.created_at >= today.strftime("%Y-%m-%d"))
                .count()
            )
            return f"TASK-{today.strftime('%Y%m%d')}-{count + 1:05d}"

    def add_audit(self, action: str, target_type: str = None,
                  target_id=None, detail: dict = None) -> None:
        from ..db.models import AuditLog
        with session_scope() as s:
            s.add(AuditLog(
                ts=utcnow(),
                actor="local",
                action=action,
                target_type=target_type,
                target_id=str(target_id) if target_id is not None else None,
                detail=detail,
            ))

    def list_audit(self, limit: int = 50, offset: int = 0) -> Dict:
        from ..db.models import AuditLog
        with session_scope() as s:
            total = s.query(AuditLog).count()
            rows = (
                s.query(AuditLog)
                .order_by(AuditLog.ts.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )
            return {
                "total": total,
                "items": [
                    {
                        "id": r.id,
                        "ts": r.ts,
                        "actor": r.actor,
                        "action": r.action,
                        "target_type": r.target_type,
                        "target_id": r.target_id,
                        "detail": r.detail,
                    }
                    for r in rows
                ],
            }

    def register_worker(self, worker: dict) -> dict:
        from ..db.models import Worker
        worker_id = worker.get("id") or uuid.uuid4()
        with session_scope() as s:
            s.add(Worker(
                id=worker_id,
                name=worker["name"],
                node=worker.get("node"),
                hostname=worker.get("hostname"),
                ip=worker.get("ip"),
                python_version=worker.get("python_version"),
                os=worker.get("os"),
                worker_version=worker.get("worker_version"),
                capabilities=worker.get("capabilities", []),
                status="IDLE",
                capacity=worker.get("capacity", 1),
                last_heartbeat_at=utcnow(),
                registered_at=utcnow(),
            ))
        return {"id": worker_id, **worker}

    def get_worker(self, worker_id) -> Optional[dict]:
        from ..db.models import Worker
        with session_scope() as s:
            r = s.query(Worker).filter(Worker.id == worker_id).first()
            if r is None:
                return None
            return _worker_to_dict(r)

    def list_workers(self) -> List[dict]:
        from ..db.models import Worker
        with session_scope() as s:
            rows = s.query(Worker).order_by(Worker.registered_at.desc()).all()
            return [_worker_to_dict(r) for r in rows]


def _job_to_dict(job: TaskJob) -> dict:
    return {
        "id": job.id,
        "group_id": job.group_id,
        "task_no": job.task_no,
        "task_type": job.task_type,
        "params": job.params,
        "status": job.status,
        "priority": job.priority,
        "required_capability": job.required_capability,
        "rate_group": job.rate_group,
        "assigned_worker_id": job.assigned_worker_id,
        "node": job.node,
        "pid": job.pid,
        "exit_code": job.exit_code,
        "attempt_no": job.attempt_no,
        "retry_count": job.retry_count,
        "max_retry": job.max_retry,
        "parent_job_id": job.parent_job_id,
        "depends_on_job_id": job.depends_on_job_id,
        "workflow_id": job.workflow_id,
        "progress": job.progress,
        "heartbeat_at": job.heartbeat_at,
        "cancel_requested": job.cancel_requested,
        "error": job.error,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "updated_at": job.updated_at,
    }


def _worker_to_dict(w) -> dict:
    return {
        "id": w.id,
        "name": w.name,
        "node": w.node,
        "hostname": w.hostname,
        "ip": w.ip,
        "python_version": w.python_version,
        "os": w.os,
        "worker_version": w.worker_version,
        "capabilities": w.capabilities,
        "status": w.status,
        "capacity": w.capacity,
        "last_heartbeat_at": w.last_heartbeat_at,
        "current_task_count": w.current_task_count,
        "last_error": w.last_error,
        "registered_at": w.registered_at,
    }


class InMemoryJobStore(JobStore):
    """内存实现（测试用）：行为与 Postgres 对齐，线程安全"""

    def __init__(self):
        self._jobs: Dict[str, dict] = {}
        self._attempts: List[dict] = []
        self._attempt_seq = 0
        self._lock = False

    def _now(self):
        return utcnow()

    def create_job(self, job: dict) -> dict:
        job_id = job.get("id") or uuid.uuid4()
        now = self._now()
        record = {
            "id": job_id,
            "group_id": job.get("group_id"),
            "task_no": job.get("task_no", f"TASK-{now.strftime('%Y%m%d')}-{len(self._jobs)+1:05d}"),
            "task_type": job["task_type"],
            "params": job.get("params", {}),
            "status": job.get("status", "QUEUED"),
            "priority": job.get("priority", 0),
            "required_capability": job.get("required_capability"),
            "rate_group": job.get("rate_group"),
            "assigned_worker_id": None,
            "node": None,
            "pid": None,
            "exit_code": None,
            "attempt_no": 0,
            "retry_count": job.get("retry_count", 0),
            "max_retry": job.get("max_retry", 0),
            "parent_job_id": job.get("parent_job_id"),
            "depends_on_job_id": job.get("depends_on_job_id"),
            "workflow_id": job.get("workflow_id"),
            "progress": None,
            "heartbeat_at": None,
            "cancel_requested": bool(job.get("cancel_requested", False)),
            "error": None,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "updated_at": now,
        }
        self._jobs[str(job_id)] = record
        return dict(record)

    def claim_next(self, worker_id, capabilities: List[str]) -> Optional[dict]:
        caps = set(capabilities or [])
        candidates = [
            j for j in self._jobs.values()
            if j["status"] == "QUEUED"
            and (j["required_capability"] is None
                 or j["required_capability"] in caps)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda j: (-j["priority"], j["created_at"]))
        job = candidates[0]
        job["status"] = "ASSIGNED"
        job["assigned_worker_id"] = worker_id
        job["attempt_no"] = job["attempt_no"] + 1
        job["started_at"] = self._now()
        job["updated_at"] = self._now()
        return dict(job)

    def update_status(
        self, job_id, from_state: str, to_state: str, retry: bool = False, **fields
    ) -> dict:
        job = self._jobs.get(str(job_id))
        if job is None or job["status"] != from_state:
            raise IllegalTransitionError(f"{from_state} → {to_state}")
        job["status"] = to_state
        job["updated_at"] = self._now()
        if to_state in TERMINAL_STATES:
            job["finished_at"] = self._now()
        if to_state == "QUEUED" and retry:
            job["retry_count"] = job["retry_count"] + 1
            job["started_at"] = None
            job["finished_at"] = None
        for k, v in fields.items():
            if v is not None:
                job[k] = v
        return dict(job)

    def append_progress(self, job_id, event: dict) -> None:
        job = self._jobs.get(str(job_id))
        if job:
            job["progress"] = event
            job["updated_at"] = self._now()

    def heartbeat(self, job_id) -> None:
        job = self._jobs.get(str(job_id))
        if job and job["status"] in ("ASSIGNED", "RUNNING"):
            job["heartbeat_at"] = self._now()

    def worker_heartbeat(self, worker_id) -> None:
        pass

    def mark_stale(self, worker_id, heartbeat_before: datetime,
                   name: str = None, node: str = None) -> int:
        count = 0
        for job in self._jobs.values():
            if job["status"] not in ("ASSIGNED", "RUNNING"):
                continue
            if job["assigned_worker_id"] != worker_id:
                if not name:
                    continue
                w = self._workers.get(str(job["assigned_worker_id"])) if hasattr(self, "_workers") else None
                if w is None or w.get("name") != name:
                    continue
                if node and w.get("node") != node:
                    continue
            if job["heartbeat_at"] is not None and job["heartbeat_at"] >= heartbeat_before:
                continue
            job["status"] = "INTERRUPTED"
            job["finished_at"] = self._now()
            job["updated_at"] = self._now()
            count += 1
        return count

    def create_attempt(self, job_id, attempt: dict) -> int:
        self._attempt_seq += 1
        self._attempts.append({
            "id": self._attempt_seq,
            "job_id": job_id,
            "attempt_no": attempt["attempt_no"],
            "worker_id": attempt.get("worker_id"),
            "pid": attempt.get("pid"),
            "started_at": attempt.get("started_at") or self._now(),
            "finished_at": None,
            "exit_code": None,
            "log_path": attempt.get("log_path"),
            "progress_path": attempt.get("progress_path"),
            "error": None,
        })
        return self._attempt_seq

    def finish_attempt(self, attempt_id: int, **fields) -> None:
        for a in self._attempts:
            if a["id"] == attempt_id:
                a.update(fields)
                return

    def get(self, job_id) -> Optional[dict]:
        job = self._jobs.get(str(job_id))
        return dict(job) if job else None

    def list(self, status: Optional[str] = None, limit: int = 50, offset: int = 0,
             task_type: Optional[str] = None, group_id=None) -> Dict:
        items = list(self._jobs.values())
        if status:
            items = [j for j in items if j["status"] == status]
        if task_type:
            items = [j for j in items if j["task_type"] == task_type]
        if group_id is not None:
            items = [j for j in items if j["group_id"] == group_id]
        items.sort(key=lambda j: j["created_at"], reverse=True)
        total = len(items)
        return {"total": total, "items": [dict(j) for j in items[offset:offset + limit]]}

    def list_attempts(self, job_id) -> List[dict]:
        attempts = [a for a in self._attempts if a["job_id"] == job_id]
        attempts.sort(key=lambda a: a["attempt_no"])
        return [dict(a) for a in attempts]

    def touch_worker_status(self, worker_id, status: str, **fields) -> None:
        pass

    def count(self, status: str) -> int:
        return sum(1 for j in self._jobs.values() if j["status"] == status)

    def next_task_no(self) -> str:
        today = utcnow().date()
        count = sum(
            1 for j in self._jobs.values()
            if j["created_at"].date() == today
        )
        return f"TASK-{today.strftime('%Y%m%d')}-{count + 1:05d}"

    def add_audit(self, action: str, target_type: str = None,
                  target_id=None, detail: dict = None) -> None:
        if not hasattr(self, "_audits"):
            self._audits = []
        self._audits.append({
            "ts": self._now(),
            "actor": "local",
            "action": action,
            "target_type": target_type,
            "target_id": str(target_id) if target_id is not None else None,
            "detail": detail,
        })

    def list_audit(self, limit: int = 50, offset: int = 0) -> Dict:
        audits = getattr(self, "_audits", [])
        audits = list(reversed(audits))
        return {"total": len(audits), "items": audits[offset:offset + limit]}

    def register_worker(self, worker: dict) -> dict:
        worker_id = worker.get("id") or uuid.uuid4()
        if not hasattr(self, "_workers"):
            self._workers = {}
        self._workers[str(worker_id)] = {
            "id": worker_id,
            "name": worker["name"],
            "node": worker.get("node"),
            "hostname": worker.get("hostname"),
            "ip": worker.get("ip"),
            "python_version": worker.get("python_version"),
            "os": worker.get("os"),
            "worker_version": worker.get("worker_version"),
            "capabilities": worker.get("capabilities", []),
            "status": "IDLE",
            "capacity": worker.get("capacity", 1),
            "last_heartbeat_at": self._now(),
            "current_task_count": 0,
            "last_error": None,
            "registered_at": self._now(),
        }
        return dict(self._workers[str(worker_id)])

    def get_worker(self, worker_id) -> Optional[dict]:
        w = self._workers.get(str(worker_id))
        return dict(w) if w else None

    def list_workers(self) -> List[dict]:
        return [dict(w) for w in getattr(self, "_workers", {}).values()]
