"""任务 API 路由：提交 / 批量 / 查询 / 取消 / 暂停 / 日志 / attempt"""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import ValidationError

from ...utils.logger import get_logger
from ...utils.progress import tail_log
from ..schemas import TaskBatchCreate, TaskCreate

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["tasks"])


def _manager(request: Request):
    return request.app.state.manager


def _validation_error(e: ValidationError):
    """pydantic v2 的 ctx.error 是 ValueError 实例，需转字符串才能 JSON 序列化"""
    errors = e.errors()
    for err in errors:
        ctx = err.get("ctx")
        if isinstance(ctx, dict):
            err["ctx"] = {
                k: str(v) if isinstance(v, Exception) else v
                for k, v in ctx.items()
            }
    return HTTPException(422, detail=errors)


@router.post("/tasks")
def create_task(req: TaskCreate, request: Request):
    try:
        job = _manager(request).submit(req.task_type, req.params)
    except ValidationError as e:
        raise _validation_error(e)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    return job


@router.post("/tasks/batch")
def create_batch(req: TaskBatchCreate, request: Request):
    try:
        result = _manager(request).batch(req.task_type, req.params_list)
    except ValidationError as e:
        raise _validation_error(e)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    return result


@router.get("/tasks")
def list_tasks(
    request: Request,
    status: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None),
    group_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return _manager(request).list(
        status=status, limit=limit, offset=offset,
        task_type=task_type, group_id=group_id,
    )


@router.get("/tasks/{job_id}")
def get_task(job_id: str, request: Request):
    job = _manager(request).get(job_id)
    if job is None:
        raise HTTPException(404, f"任务不存在: {job_id}")
    job["attempts"] = _manager(request).attempts(job_id)
    return job


@router.post("/tasks/{job_id}/stop")
def stop_task(job_id: str, request: Request):
    try:
        job = _manager(request).cancel(job_id)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return job


@router.post("/tasks/{job_id}/pause")
def pause_task(job_id: str, request: Request):
    try:
        job = _manager(request).pause(job_id)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return job


@router.post("/tasks/{job_id}/resume")
def resume_task(job_id: str, request: Request):
    try:
        job = _manager(request).resume(job_id)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return job


@router.get("/tasks/{job_id}/log")
def task_log(
    job_id: str,
    request: Request,
    offset: int = Query(0, ge=0),
    attempt: Optional[int] = Query(None),
):
    mgr = _manager(request)
    attempts = mgr.attempts(job_id)
    if not attempts:
        raise HTTPException(404, f"任务无执行记录: {job_id}")
    chosen = None
    if attempt is not None:
        chosen = next((a for a in attempts if a["attempt_no"] == attempt), None)
        if chosen is None:
            raise HTTPException(404, f"attempt 不存在: {attempt}")
    else:
        chosen = attempts[-1]
    log_path = chosen.get("log_path")
    if not log_path or not os.path.exists(log_path):
        return {"content": "", "offset": offset, "size": 0, "attempt_no": chosen["attempt_no"]}
    result = tail_log(log_path, offset=offset)
    result["attempt_no"] = chosen["attempt_no"]
    return result


@router.get("/tasks/{job_id}/attempts")
def task_attempts(job_id: str, request: Request):
    return _manager(request).attempts(job_id)
