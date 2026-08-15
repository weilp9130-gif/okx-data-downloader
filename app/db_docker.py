"""Docker数据库引导

在下载/同步代码运行前，自动检测并确保 PostgreSQL + TimescaleDB 可用：

1. 若配置的数据库地址(DB_HOST:DB_PORT)已可连接 -> 直接使用，不打扰现有数据库
2. 若不可达且未禁用Docker(DB_USE_DOCKER 默认 auto) -> 检测Docker，
   用 timescale/timescaledb 镜像启动(或复用已存在的)容器，等待就绪后返回
3. 若Docker不可用或配置为禁用 -> 抛出明确错误，提示如何修复

生效方式：ensure_database() 已挂在 database.get_engine() 内，
download_all.py / main.py / sync_daemon.py 等所有入口自动生效。
"""

import shutil
import socket
import subprocess
import threading
import time
from typing import List, Tuple

from .config import Config
from .utils.logger import get_logger

logger = get_logger(__name__)

# 只有本机地址才允许自动用Docker启动（远程数据库需用户自行处理）
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

_started = False
_start_lock = threading.Lock()


def _run(args: List[str], timeout: float = 120.0) -> Tuple[int, str, str]:
    """执行命令，返回 (returncode, stdout, stderr)"""
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def docker_available() -> bool:
    """检测 docker CLI 是否存在且守护进程可用"""
    if shutil.which("docker") is None:
        return False
    try:
        rc, _, _ = _run(
            ["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15
        )
        return rc == 0
    except Exception:
        return False


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _container_running(name: str) -> bool:
    rc, out, _ = _run(
        ["docker", "ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}"]
    )
    return rc == 0 and out == name


def _container_exists(name: str) -> bool:
    rc, out, _ = _run(
        ["docker", "ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}"]
    )
    return rc == 0 and out == name


def _start_container(cfg) -> None:
    name = cfg.container_name
    if _container_running(name):
        logger.info(f"Docker数据库容器 {name} 已在运行")
        return
    if _container_exists(name):
        logger.info(f"启动已存在的Docker数据库容器 {name} ...")
        rc, out, err = _run(["docker", "start", name], timeout=120)
        if rc != 0:
            raise RuntimeError(f"启动Docker容器 {name} 失败: {err or out}")
        return
    logger.info(f"创建TimescaleDB容器 {name} (镜像 {cfg.image}) ...")
    # 注意: 数据卷挂载路径 /var/lib/postgresql/data 适用于 PG16 镜像;
    # 若自定义镜像版本不同(如 PG18 改为 /var/lib/postgresql), 需同步调整。
    cmd = [
        "docker", "run", "-d", "--name", name,
        "--restart", "unless-stopped",
        "--shm-size", "2g",
        "-e", f"POSTGRES_USER={cfg.user}",
        "-e", f"POSTGRES_PASSWORD={cfg.password}",
        "-e", f"POSTGRES_DB={cfg.name}",
        "-p", f"127.0.0.1:{cfg.port}:5432",
        "-v", f"{cfg.data_volume}:/var/lib/postgresql/data",
        cfg.image,
        "postgres",
        "-c", "max_connections=200",
        "-c", "work_mem=4MB",
        "-c", "max_wal_size=2GB",
    ]
    # 首次运行需拉取镜像，放宽超时到10分钟
    rc, out, err = _run(cmd, timeout=600)
    if rc != 0:
        raise RuntimeError(f"创建Docker容器 {name} 失败: {err or out}")


def _probe_real_connection(cfg, timeout: float = 4.0) -> bool:
    """用配置的真实凭据建立一次应用级连接，确认数据库真正可用

    pg_isready 只探测 TCP 握手，timescale 镜像的 entrypoint 在初始化完成后
    会先停机一次再正式启动，可能误判。真实连接探测同时验证了凭据正确性。
    """
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=cfg.host, port=cfg.port, user=cfg.user,
            password=cfg.password, dbname=cfg.name,
            connect_timeout=int(timeout),  # libpq要求整数字符串
        )
        conn.close()
        return True
    except Exception as e:
        logger.debug("数据库连接探测失败(%s:%s/%s): %s",
                     cfg.host, cfg.port, cfg.name, e)
        return False


def _wait_db_ready(cfg, timeout: float = 120.0) -> bool:
    """等待容器就绪：pg_isready 通过后再做一次真实连接探测"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            rc, _, _ = _run(
                ["docker", "exec", cfg.container_name, "pg_isready",
                 "-U", cfg.user, "-d", cfg.name],
                timeout=30,
            )
        except Exception as e:
            # docker exec 瞬态失败(如容器仍在启动)时忽略并重试
            logger.debug("pg_isready 探测失败，重试: %s", e)
            time.sleep(2)
            continue
        if rc == 0 and _probe_real_connection(cfg):
            return True
        time.sleep(2)
    return False


def ensure_database() -> None:
    """下载前确保数据库可用（幂等，进程内只执行一次）"""
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        _started = True
        cfg = Config().database

        if _port_open(cfg.host, cfg.port):
            logger.info(f"数据库 {cfg.host}:{cfg.port} 已在监听，跳过Docker引导")
            return

        use_docker = str(cfg.use_docker).strip().lower() != "false"
        if not use_docker:
            raise RuntimeError(
                f"数据库 {cfg.host}:{cfg.port} 不可达，且 DB_USE_DOCKER=false。"
                "请先启动数据库服务，或设置 DB_USE_DOCKER=true 让程序自动用Docker启动。"
            )
        if cfg.host not in _LOCAL_HOSTS:
            raise RuntimeError(
                f"数据库 {cfg.host}:{cfg.port} 不可达。非本机地址不会自动启动Docker，"
                "请先手动启动远程数据库。"
            )
        if not docker_available():
            raise RuntimeError(
                "数据库不可达，且未检测到可用的Docker。请安装并启动 Docker Desktop，"
                "或配置现有数据库(DB_HOST/DB_PORT/DB_USER/DB_PASSWORD)。"
            )
        logger.info("本地数据库不可达，通过Docker启动TimescaleDB ...")
        _start_container(cfg)
        if not _wait_db_ready(cfg):
            raise RuntimeError(
                f"等待Docker数据库就绪超时(容器 {cfg.container_name})。"
                "请运行 docker logs 检查初始化日志。"
            )
        logger.info(f"Docker数据库就绪: {cfg.host}:{cfg.port}/{cfg.name}")


if __name__ == "__main__":
    ensure_database()
