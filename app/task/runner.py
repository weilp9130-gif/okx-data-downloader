"""TaskRunner：对单个已认领 job 执行子进程并跟踪终态

    - Popen（shell=False，字节文件句柄 stdout/stderr）
    - 增量消费 progress JSONL → 写 DB jobs.progress（节流 ~1/2s）
    - 轮询 cancel_requested → terminate 子进程 → CANCELLED
    - 终态 finalize：exit_code/attempt 记录/SUCCESS/FAILED/retry/on_success
    - 终态删除 progress 文件；out.log 长期保留
"""

import subprocess
import time
from pathlib import Path
from typing import Optional

from ..utils.logger import get_logger
from ..utils.progress import read_progress_events, remove_progress_file, tail_log
from . import registry
from .store import JobStore, utcnow

logger = get_logger(__name__)

PROGRESS_POLL_SEC = 0.5
CANCEL_POLL_SEC = 2.0
PROGRESS_DB_THROTTLE_SEC = 0.5
CANCEL_KILL_AFTER_SEC = 8.0

# 项目根目录（app/task/runner.py → 项目根）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TaskRunner:
    """子进程执行器"""

    def __init__(self, store: JobStore, runtime_root: str = "runtime/jobs"):
        self.store = store
        self.runtime_root = Path(runtime_root)

    # ------------------------------------------------------------------
    # 路径
    # ------------------------------------------------------------------
    def job_dir(self, job_id) -> Path:
        d = self.runtime_root / str(job_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def log_path(self, job_id, attempt_no) -> Path:
        return self.job_dir(job_id) / f"attempt-{attempt_no}.out.log"

    def progress_path(self, job_id, attempt_no) -> Path:
        return self.job_dir(job_id) / f"attempt-{attempt_no}.jsonl"

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def run(self, job: dict, worker_id) -> None:
        """执行单个已认领（ASSIGNED）任务直至终态"""
        job_id = job["id"]
        attempt_no = job["attempt_no"]

        # 领取后未启动即被取消 → ASSIGNED→CANCELLED
        if job.get("cancel_requested"):
            logger.info("任务领取后已被取消，不启动: %s", job_id)
            try:
                self.store.update_status(job_id, "ASSIGNED", "CANCELLED")
            except Exception as e:
                logger.warning("取消已领取任务失败: %s | %s", job_id, e)
            return

        spec = registry.get_spec(job["task_type"])
        if spec is None:
            self._fail_start(job_id, attempt_no, worker_id,
                             f"未知任务类型: {job['task_type']}")
            return

        log_path = self.log_path(job_id, attempt_no)
        progress_path = self.progress_path(job_id, attempt_no)

        try:
            argv = spec.command_argv(job["params"], str(progress_path))
        except Exception as e:
            self._fail_start(job_id, attempt_no, worker_id, f"argv 构造失败: {e}")
            return

        attempt_id = self.store.create_attempt(job_id, {
            "attempt_no": attempt_no,
            "worker_id": worker_id,
            "pid": None,
            "log_path": str(log_path),
            "progress_path": str(progress_path),
        })

        # 启动子进程（字节句柄）
        try:
            with open(log_path, "wb") as out:
                proc = subprocess.Popen(
                    argv,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    shell=False,
                    cwd=str(PROJECT_ROOT),
                )
        except Exception as e:
            logger.error("子进程启动失败: %s | %s", job_id, e)
            self._fail_start(job_id, attempt_no, worker_id, f"子进程启动失败: {e}",
                             attempt_id=attempt_id)
            return

        # 状态 ASSIGNED→RUNNING
        try:
            self.store.update_status(job_id, "ASSIGNED", "RUNNING", pid=proc.pid)
            self.store.finish_attempt(attempt_id, pid=proc.pid)
        except Exception as e:
            logger.error("状态流转 ASSIGNED→RUNNING 失败: %s | %s", job_id, e)
            try:
                proc.terminate()
            except OSError:
                pass
            return

        logger.info(
            "任务开始执行: %s | %s | attempt=%d | pid=%d | argv=%s",
            job["task_no"], job["task_type"], attempt_no, proc.pid, argv,
        )

        cancelled = self._monitor(job_id, proc, progress_path)

        # 进程结束后补读一次 progress（极短任务可能未被轮询捕获）
        try:
            events, _ = read_progress_events(progress_path)
            if events:
                self.store.append_progress(job_id, events[-1])
        except Exception:
            pass

        # 终态 finalize
        self._finalize(job_id, attempt_id, attempt_no, worker_id,
                       proc, spec, progress_path, cancelled=cancelled)

    # ------------------------------------------------------------------
    # 启动失败处理
    # ------------------------------------------------------------------
    def _fail_start(self, job_id, attempt_no, worker_id, error: str,
                    attempt_id: Optional[int] = None) -> None:
        if attempt_id is None:
            attempt_id = self.store.create_attempt(job_id, {
                "attempt_no": attempt_no,
                "worker_id": worker_id,
                "started_at": utcnow(),
            })
        self.store.finish_attempt(attempt_id, finished_at=utcnow(),
                                  exit_code=-1, error=error[:2000])
        try:
            self.store.update_status(job_id, "ASSIGNED", "FAILED", error=error[:2000])
        except Exception as e:
            logger.warning("启动失败状态流转失败: %s | %s", job_id, e)

    # ------------------------------------------------------------------
    # 运行期监控：进度 + 取消轮询
    # ------------------------------------------------------------------
    def _monitor(self, job_id, proc, progress_path: Path) -> bool:
        cancelled = False
        offset = 0
        last_db = time.monotonic()
        last_hb = time.monotonic()
        last_progress = None
        cancel_started_at: Optional[float] = None

        while proc.poll() is None:
            # 消费 progress JSONL
            try:
                events, new_offset = read_progress_events(progress_path, start_offset=offset)
                if events:
                    offset = new_offset
                    last_progress = events[-1]
            except OSError:
                pass

            # 写 DB progress（节流 ~1/2s）
            if last_progress is not None and time.monotonic() - last_db >= PROGRESS_DB_THROTTLE_SEC:
                try:
                    self.store.append_progress(job_id, last_progress)
                except Exception as e:
                    logger.warning("progress 写库失败: %s | %s", job_id, e)
                last_db = time.monotonic()

            # 任务心跳（~10s，供重启 recovery 判定残留任务）
            if time.monotonic() - last_hb >= 10.0:
                try:
                    self.store.heartbeat(job_id)
                except Exception:
                    pass
                last_hb = time.monotonic()

            # 取消轮询
            if not cancelled:
                try:
                    job = self.store.get(job_id)
                    if job is not None and job.get("cancel_requested"):
                        logger.info("收到取消请求，终止子进程: %s", job_id)
                        cancelled = True
                        cancel_started_at = time.monotonic()
                        try:
                            proc.terminate()
                        except OSError:
                            pass
                except Exception:
                    pass

            if (
                cancelled
                and cancel_started_at is not None
                and time.monotonic() - cancel_started_at >= CANCEL_KILL_AFTER_SEC
                and proc.poll() is None
            ):
                logger.warning("任务未在宽限期内退出，强制结束: %s", job_id)
                try:
                    proc.kill()
                except OSError:
                    pass
                cancel_started_at = None

            time.sleep(PROGRESS_POLL_SEC)

        # 进程已结束：刷新最后一次进度
        if last_progress is not None:
            try:
                self.store.append_progress(job_id, last_progress)
            except Exception:
                pass
        return cancelled

    # ------------------------------------------------------------------
    # 终态
    # ------------------------------------------------------------------
    def _finalize(self, job_id, attempt_id, attempt_no, worker_id,
                  proc, spec, progress_path: Path, cancelled: bool) -> None:
        exit_code = proc.poll()
        if exit_code is None:
            exit_code = proc.wait()
        now = utcnow()

        error = None
        if exit_code != 0:
            error = self._tail_error(self.log_path(job_id, attempt_no))

        self.store.finish_attempt(
            attempt_id,
            finished_at=now,
            exit_code=exit_code,
            error=(error[:2000] if error else None),
        )
        remove_progress_file(progress_path)

        job = self.store.get(job_id)
        if job is None:
            return

        try:
            if cancelled:
                self.store.update_status(job_id, "RUNNING", "CANCELLED",
                                         exit_code=exit_code)
                self.store.add_audit("CANCEL_TASK", "job", job_id,
                                     {"via_worker": True})
            elif exit_code == 0:
                self.store.update_status(job_id, "RUNNING", "SUCCESS",
                                         exit_code=exit_code)
                attempt = {
                    "attempt_no": attempt_no,
                    "log_path": str(self.log_path(job_id, attempt_no)),
                    "progress_path": str(progress_path),
                }
                if spec.on_success is not None:
                    try:
                        spec.on_success(self.store, self.store.get(job_id), attempt)
                    except Exception as e:
                        logger.error("on_success 钩子失败: %s | %s", job_id, e)
            else:
                # FAILED → 可重试则回队（新 attempt）
                if job["retry_count"] < job["max_retry"]:
                    self.store.update_status(job_id, "RUNNING", "FAILED",
                                             error=error[:2000] if error else None,
                                             exit_code=exit_code)
                    self.store.update_status(job_id, "FAILED", "QUEUED", retry=True,
                                             error=None)
                    self.store.add_audit("RETRY_TASK", "job", job_id, {
                        "attempt_no": attempt_no,
                        "exit_code": exit_code,
                    })
                    logger.warning("任务失败，进入重试: %s | attempt=%d → %d",
                                   job_id, attempt_no, job["retry_count"] + 1)
                else:
                    self.store.update_status(job_id, "RUNNING", "FAILED",
                                             error=error[:2000] if error else None,
                                             exit_code=exit_code)
                    logger.error("任务失败: %s | attempt=%d | exit=%s | %s",
                                 job_id, attempt_no, exit_code, error or "")
        except Exception as e:
            logger.error("终态 finalize 失败: %s | %s", job_id, e)

    def _tail_error(self, log_path: Path, limit: int = 2000) -> Optional[str]:
        """从 out.log 尾部取错误摘要"""
        try:
            result = tail_log(str(log_path), offset=max(0, log_path.stat().st_size - 4000))
            content = result["content"].strip()
            return content[-limit:] if content else None
        except OSError:
            return None
