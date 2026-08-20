"""WebSocket Hub：DB 轮询广播（独立线程）

Worker 是独立进程（与 FastAPI 无 HTTP 耦合），因此 WS 不做直接事件推送：
Hub 线程每 1~2s 查 jobs/workers 表（按 updated_at 变化）→ 广播 job_update /
worker_update；心跳 30s 发 ping。日志流不走 WS（由 REST 字节偏移轮询）。

客户端断线由前端指数退避重连。
"""

import asyncio
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from fastapi import WebSocket
from sqlalchemy.exc import SQLAlchemyError

from ..utils.logger import get_logger

logger = get_logger(__name__)


def _serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class ConnectionManager:
    """跟踪在线 WebSocket 客户端"""

    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = threading.Lock()

    def connect(self, ws: WebSocket) -> None:
        with self._lock:
            self._connections.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        with self._lock:
            self._connections.discard(ws)

    async def send(self, message: dict) -> None:
        with self._lock:
            targets = list(self._connections)
        for ws in targets:
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(ws)

    def count(self) -> int:
        with self._lock:
            return len(self._connections)


class JobHub:
    """DB 轮询 Hub：独立线程 + 自身 asyncio 事件循环"""

    POLL_INTERVAL = 1.5
    PING_INTERVAL = 30.0

    def __init__(self, store, poll_interval: float = POLL_INTERVAL):
        self.store = store
        self.poll_interval = poll_interval
        self.manager = ConnectionManager()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._job_seen: Dict[str, str] = {}
        self._worker_seen: Dict[str, str] = {}
        self._db_ok = True

    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="ws-hub", daemon=True)
        self._thread.start()
        logger.info("WS Hub 已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_loop())
        except Exception as e:
            logger.error("WS Hub 循环异常退出: %s", e)
        finally:
            self._loop.close()

    async def _async_loop(self) -> None:
        last_ping = time.monotonic()
        while not self._stop.is_set():
            await asyncio.sleep(self.poll_interval)
            messages = await self._poll()
            for msg in messages:
                await self.manager.send(msg)
            if time.monotonic() - last_ping >= self.PING_INTERVAL:
                last_ping = time.monotonic()
                await self.manager.send({"type": "ping", "data": {"ts": time.time()}})

    # ------------------------------------------------------------------
    def _inst_of(self, job: dict) -> Optional[str]:
        params = job.get("params") or {}
        if "inst" in params:
            return params["inst"]
        insts = params.get("insts")
        if isinstance(insts, list) and insts:
            return insts[0]
        return None

    async def _poll(self) -> List[dict]:
        messages: List[dict] = []
        try:
            jobs = self.store.list(limit=500)["items"]
            for j in jobs:
                key = str(j["id"])
                marker = (j.get("updated_at") or j.get("created_at"))
                marker = _serialize(marker)
                stamp = f"{marker}|{j['status']}|{j['attempt_no']}"
                if self._job_seen.get(key) == stamp:
                    continue
                self._job_seen[key] = stamp
                messages.append({
                    "type": "job_update",
                    "data": {
                        "id": key,
                        "task_no": j["task_no"],
                        "task_type": j["task_type"],
                        "inst": self._inst_of(j),
                        "status": j["status"],
                        "progress": j.get("progress"),
                        "attempt_no": j.get("attempt_no", 0),
                        "group_id": _serialize(j.get("group_id")),
                        "error": j.get("error"),
                    },
                })

            workers = self.store.list_workers()
            for w in workers:
                key = str(w["id"])
                marker = _serialize(w.get("last_heartbeat_at"))
                stamp = f"{marker}|{w['status']}|{w.get('current_task_count', 0)}"
                if self._worker_seen.get(key) == stamp:
                    continue
                self._worker_seen[key] = stamp
                messages.append({
                    "type": "worker_update",
                    "data": {
                        "id": key,
                        "name": w["name"],
                        "node": w.get("node"),
                        "hostname": w.get("hostname"),
                        "status": w.get("status"),
                        "last_heartbeat_at": marker,
                        "current_task_count": w.get("current_task_count", 0),
                        "capabilities": w.get("capabilities", []),
                    },
                })
            if messages and not self._db_ok:
                logger.info("WS Hub DB 恢复")
            self._db_ok = True
        except (SQLAlchemyError, Exception) as e:
            if self._db_ok:
                logger.warning("WS Hub DB 轮询失败: %s", e)
            self._db_ok = False
        return messages
