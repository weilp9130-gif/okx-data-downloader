"""系统服务：健康检查 + 打码配置 + 日志读取（白名单防路径穿越）"""

import os
import platform
from pathlib import Path
from typing import Dict, List, Optional

from ..config.config import Config
from ..utils.logger import LOG_DIR, get_logger

logger = get_logger(__name__)

VERSION = "1.0.0"

# 需要打码的环境变量（只显示前后缀）
_MASKED_KEYS = ("OKX_SECRET_KEY", "OKX_PASSPHRASE", "OKX_API_KEY", "DB_PASSWORD")

# 可读取的日志文件扩展名白名单
_ALLOWED_LOG_SUFFIXES = (".log",)


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def system_info() -> Dict:
    """只读配置（密钥打码）+ 版本"""
    cfg = Config()
    masked = {}
    for name in sorted(_MASKED_KEYS):
        masked[name] = _mask(os.getenv(name, ""))
    env_summary = {
        "WEBUI_HOST": os.getenv("WEBUI_HOST", "127.0.0.1"),
        "WEBUI_PORT": os.getenv("WEBUI_PORT", "8000"),
        "DB_HOST": cfg.database.host,
        "DB_NAME": cfg.database.name,
        "TIMEZONE": cfg.timezone,
    }
    return {
        "version": VERSION,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "db_host": cfg.database.host,
        "db_name": cfg.database.name,
        "okx": {
            "base_url": cfg.okx.base_url,
            "sandbox": cfg.okx.sandbox,
            "rate_limit_per_second": cfg.okx.rate_limit_per_second,
            "ip_rate_limit_per_second": cfg.okx.ip_rate_limit_per_second,
            "proxy_urls_count": len([u for u in cfg.okx.proxy_urls.split(",") if u]),
        },
        "download": {
            "default_instrument": cfg.download.default_instrument,
            "default_bar": cfg.download.default_bar,
            "kline_bars": cfg.download.kline_bars,
        },
        "masked_keys": masked,
        "env": env_summary,
    }


def list_log_files() -> List[Dict]:
    """列出 logs/ 目录下的日志文件"""
    if not LOG_DIR.exists():
        return []
    files = []
    for p in sorted(LOG_DIR.glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True):
        files.append({
            "name": p.name,
            "size": p.stat().st_size,
            "modified": p.stat().st_mtime,
        })
    return files[:200]


def resolve_log_path(file: str) -> Optional[Path]:
    """按文件名白名单解析日志路径，防路径穿越

    - 只允许 logs/ 目录下的 *.log 文件（basename 校验）
    """
    if not file:
        return None
    name = Path(file).name
    if name != file or not name.endswith(_ALLOWED_LOG_SUFFIXES):
        return None
    p = LOG_DIR / name
    if not p.exists():
        return None
    return p


def read_log(file: str, offset: int = 0) -> Dict:
    """按字节偏移读取日志（白名单）"""
    from ..utils.progress import tail_log

    p = resolve_log_path(file)
    if p is None:
        raise ValueError(f"非法日志文件: {file!r}")
    result = tail_log(str(p), offset=offset)
    result["file"] = file
    return result
