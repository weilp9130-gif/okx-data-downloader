"""系统 API 路由：健康检查 / 只读配置 / 日志读取"""

from fastapi import APIRouter, HTTPException, Request, Query

from ...services.system import list_log_files, read_log, resolve_log_path, system_info
from ...services.health import get_health

router = APIRouter(prefix="/api", tags=["system"])


@router.get("/health")
def health(request: Request):
    health_snapshot = get_health(request.app.state.manager.store)
    from ...config.config import Config
    cfg = Config().database
    return {
        **health_snapshot,
        "db_host": f"{cfg.host}:{cfg.port}/{cfg.name}",
        "okx_error": None,
        "version": system_info()["version"],
    }


@router.get("/system/info")
def info():
    return system_info()


@router.get("/system/logs")
def logs(offset: int = Query(0, ge=0)):
    return {"files": list_log_files()}


@router.get("/system/log")
def log(file: str, offset: int = Query(0, ge=0)):
    if resolve_log_path(file) is None:
        raise HTTPException(404, f"日志不存在或非法: {file!r}")
    try:
        return read_log(file, offset=offset)
    except ValueError as e:
        raise HTTPException(400, str(e))
