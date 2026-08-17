"""下载范围配置：币种区域 / 时间区域

从 download_scope.toml 读取，供 sync_continuous / sync_realtime / backfill 使用。
优先级：命令行参数 > 配置文件 > 内置默认。

配置示例见项目根目录 download_scope.toml；可用环境变量 DOWNLOAD_SCOPE 指定路径。
"""

import os
import tomllib
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from .okx_client import OKXClient
from .utils.logger import get_logger
from .utils.time_utils import parse_date

logger = get_logger(__name__)

DEFAULT_SCOPE_PATH = os.getenv("DOWNLOAD_SCOPE", "download_scope.toml")

_DEFAULTS = {
    "inst_region": {
        "mode": "all",
        "include": [],
        "exclude": [],
        "inst_type": "SWAP",
        "settle_ccy": "USDT",
        "state": "live",
    },
    "time_region": {"start": "", "end": "", "limit_days": 0},
    "defaults": {"bar": "1m", "workers": 0, "channels": "trades"},
}


def load_scope(path: Optional[str] = None) -> dict:
    """读取下载范围配置（与内置默认合并）

    Args:
        path: 配置文件路径，默认取 DOWNLOAD_SCOPE 或 download_scope.toml

    Returns:
        dict: {inst_region, time_region, defaults}
    """
    p = path or DEFAULT_SCOPE_PATH
    cfg = {k: dict(v) for k, v in _DEFAULTS.items()}
    if os.path.exists(p):
        try:
            with open(p, "rb") as f:
                user = tomllib.load(f)
            for section in cfg:
                if section in user and isinstance(user[section], dict):
                    cfg[section].update(user[section])
        except Exception as e:
            logger.warning("读取下载范围配置失败(%s)，使用默认: %s", p, e)
    else:
        logger.info("未找到下载范围配置 %s，使用默认（全部USDT永续）", p)
    return cfg


def resolve_instruments(
    scope: dict,
    client: Optional[OKXClient] = None,
    explicit: Optional[List[str]] = None,
) -> List[str]:
    """解析币种区域，返回 inst_id 列表

    Args:
        scope: load_scope() 的结果
        client: OKXClient（拉取合约列表用）
        explicit: 显式指定的 inst 列表（命令行优先）

    Returns:
        List[str]: inst_id 列表
    """
    if explicit:
        return [i for i in explicit if i]

    region = scope.get("inst_region", {})
    mode = region.get("mode", "all")
    include = [i for i in region.get("include", []) if i]
    exclude = set(i for i in region.get("exclude", []) if i)

    if mode == "include":
        return [i for i in include if i not in exclude]

    client = client or OKXClient()
    inst_type = region.get("inst_type", "SWAP")
    settle_ccy = region.get("settle_ccy")
    state = region.get("state")
    try:
        data = client.get_instruments(inst_type=inst_type)
    except Exception as e:
        logger.error("获取合约列表失败: %s", e)
        return []

    inst_ids = []
    for d in data:
        if d.get("instType") != inst_type:
            continue
        if settle_ccy and d.get("settleCcy") != settle_ccy:
            continue
        if state and d.get("state") != state:
            continue
        inst_ids.append(d["instId"])
    inst_ids.sort()

    if mode == "exclude":
        return [i for i in inst_ids if i not in exclude]
    # mode == "all"：仍应用 exclude（可作为黑名单）
    return [i for i in inst_ids if i not in exclude]


def resolve_time_range(
    scope: dict,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit_days: Optional[int] = None,
) -> Tuple[Optional[datetime], datetime]:
    """解析时间区域，返回 (start, end)（UTC，aware）

    优先级：显式参数 > 配置文件 > 全部历史。

    Returns:
        (start, end)：start 为 None 表示不限起点（2019 或 listTime）
    """
    region = scope.get("time_region", {})
    cfg_start = start if start is not None else (region.get("start") or None)
    cfg_end = end if end is not None else (region.get("end") or None)
    cfg_limit = limit_days if limit_days is not None else (region.get("limit_days") or 0)

    end_dt = parse_date(cfg_end) if cfg_end else datetime.now(timezone.utc)
    if cfg_start:
        start_dt = parse_date(cfg_start)
    elif cfg_limit and cfg_limit > 0:
        start_dt = end_dt - timedelta(days=cfg_limit)
    else:
        start_dt = None  # 全部历史
    return start_dt, end_dt


def scope_default(scope: dict, key: str, fallback=None):
    """读取 defaults 节的单项配置"""
    d = scope.get("defaults", {})
    value = d.get(key)
    return value if value not in (None, "", 0) else fallback
