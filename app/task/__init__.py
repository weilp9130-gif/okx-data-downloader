"""任务域：与 web 解耦的独立任务模块

    manager        提交/取消编排（供 FastAPI）
    state_machine  状态机
    registry       TaskSpec + argv 构造 + capability + rate_group + on_success
    store          JobStore（Postgres / InMemory）
    runner         子进程执行 + 进度 + 取消 + 终态
    worker_service 独立 Worker 轮询领取
"""
