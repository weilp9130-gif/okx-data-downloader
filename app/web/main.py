"""FastAPI 装配：路由 + 静态挂载 + lifespan（init_db / dataset 种子 / WS Hub）

DB 不可达时 webui 可启动降级（相关端点 503，UI 降级）。
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from ..task.manager import TaskManager
from ..task.store import PostgresJobStore
from ..utils.logger import get_logger
from .ws import JobHub

logger = get_logger(__name__)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def create_app(store=None, seed_schema: bool = True) -> FastAPI:
    """构建 FastAPI 应用

    Args:
        store: 可注入的 JobStore（测试用 InMemory）
        seed_schema: 是否初始化 DB schema + dataset 种子（测试关闭）
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if seed_schema:
            try:
                from ..db.database import init_db
                init_db()
            except Exception as e:
                logger.warning("DB schema 初始化失败（降级启动）: %s", e)
            try:
                from ..services.assets import seed_dataset_definitions
                seed_dataset_definitions()
            except Exception as e:
                logger.warning("dataset_definition 种子失败: %s", e)
        app.state.hub.start()
        logger.info("OKX Quant Platform 启动完成")
        yield
        app.state.hub.stop()
        logger.info("OKX Quant Platform 已停止")

    app = FastAPI(
        title="OKX Quant Platform",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(OperationalError)
    @app.exception_handler(DBAPIError)
    @app.exception_handler(SQLAlchemyError)
    async def database_error_handler(request: Request, exc: SQLAlchemyError):
        logger.warning("数据库请求失败: %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "数据库暂不可用，请稍后重试",
                "code": "DATABASE_UNAVAILABLE",
                "retryable": True,
            },
        )

    app.state.store = store if store is not None else PostgresJobStore()
    app.state.manager = TaskManager(app.state.store)
    app.state.hub = JobHub(app.state.store)

    # ---- 路由 ----
    from .routers import (
        assets,
        audit,
        dashboard,
        ingestion,
        monitor,
        quality,
        system,
        tasks,
    )

    for r in (system, tasks, audit, assets, quality, monitor, dashboard, ingestion):
        app.include_router(r.router)

    # ---- WebSocket Hub ----
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        app.state.hub.manager.connect(ws)
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            app.state.hub.manager.disconnect(ws)

    # ---- 静态资源（前端 build 产物） ----
    if os.path.isdir(STATIC_DIR):
        app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
    else:
        @app.get("/")
        def root():
            return {"name": "OKX Quant Platform", "docs": "/docs",
                    "static": "前端未构建（npm run build 后由 app/web/static 托管）"}

    return app


app = create_app()
