"""审计日志 API 路由"""

from fastapi import APIRouter, Request, Query

router = APIRouter(prefix="/api", tags=["audit"])


@router.get("/audit")
def list_audit(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return request.app.state.manager.audit(limit=limit, offset=offset)
