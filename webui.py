"""OKX Quant Platform Web 后台入口（独立进程，与 worker.py 无依赖）

运行：
    python webui.py

默认监听 127.0.0.1:8000（WEBUI_HOST / WEBUI_PORT 可覆盖）。
"""

import os
import sys

os.environ.setdefault("WEBUI_HOST", "127.0.0.1")
os.environ.setdefault("WEBUI_PORT", "8000")


def main() -> int:
    import uvicorn

    from app.web.main import app

    host = os.getenv("WEBUI_HOST", "127.0.0.1")
    port = int(os.getenv("WEBUI_PORT", "8000"))
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
