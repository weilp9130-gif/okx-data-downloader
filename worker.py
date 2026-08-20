"""OKX Quant Platform 独立任务 Worker（独立进程，与 webui.py 无依赖）

运行：
    python worker.py

Worker 通过 DB 轮询领取 QUEUED 任务并执行 CLI 子进程；Web 重启不影响
运行中任务；Worker 重启时自动把自身残留任务置 INTERRUPTED。
"""

import os
import sys

os.environ.setdefault("WORKER_NAME", "worker-1")
os.environ.setdefault("WORKER_NODE", "local")

from app.task.worker_service import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
