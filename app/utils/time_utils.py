"""时间与日期工具函数"""

import time
from datetime import datetime, timezone
from typing import Optional


def utc_now() -> datetime:
    """返回当前UTC时间（带时区信息）"""
    return datetime.now(timezone.utc)


def utc_ms_timestamp(dt: Optional[datetime] = None) -> int:
    """将datetime转换为UTC毫秒时间戳

    Args:
        dt: datetime对象，默认当前UTC时间（可忽略时区）

    Returns:
        int: 毫秒级时间戳
    """
    dt = dt or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_datetime(ms: int, tz: str = "UTC") -> datetime:
    """将毫秒时间戳转换为datetime对象

    Args:
        ms: 毫秒时间戳（OKX等接口可能返回字符串，内部统一转int）
        tz: 目标时区（'UTC' 或 zoneinfo时区名）

    Returns:
        datetime（带时区信息）
    """
    if ms is None:
        return None
    ms = int(ms)
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    if tz and tz.upper() != "UTC":
        try:
            from zoneinfo import ZoneInfo
            dt = dt.astimezone(ZoneInfo(tz))
        except Exception:
            # 时区解析失败时保留UTC，不打断调用方
            pass
    return dt


def parse_date(date_str: str) -> datetime:
    """解析日期字符串为UTC datetime

    支持格式：
        - '2024-01-01'
        - '2024-01-01 12:30:00'
        - '2024-01-01T12:30:00Z'

    Args:
        date_str: 日期字符串

    Returns:
        datetime (naive UTC)
    """
    date_str = date_str.strip().replace("Z", "").replace("T", " ")
    if len(date_str) == 10:
        fmt = "%Y-%m-%d"
    elif len(date_str) == 19:
        fmt = "%Y-%m-%d %H:%M:%S"
    else:
        raise ValueError(f"无法解析日期: {date_str}")
    # 返回naive UTC时间
    return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc).replace(tzinfo=None)


def as_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """将带时区的datetime转为naive UTC datetime

    若 dt 为 None 或已是 naive，则原样返回。
    """
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def sleep_with_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> None:
    """指数退避睡眠，用于请求重试

    Args:
        attempt: 第几次重试（从0开始）
        base_delay: 基础延迟秒数
        max_delay: 最大延迟秒数
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    time.sleep(delay)


def bar_to_seconds(bar: str) -> int:
    """将K线粒度转换为秒数

    支持的粒度：
        - '1m' '3m' '5m' '15m' '30m'
        - '1H' '2H' '4H' '12H'
        - '1D' '1W' '1M'
        - OKX特有UTC后缀：'6HUtc'（UTC 6小时切割）、'1MUtc'（UTC自然月）

    Args:
        bar: 时间粒度字符串

    Returns:
        int: 秒数

    Raises:
        ValueError: 对非法bar格式返回错误
    """
    if not bar or not isinstance(bar, str):
        raise ValueError(f"无效的时间粒度: {bar}")

    # 去除 OKX 特有的 Utc 后缀（如 6HUtc → 6H）
    base = bar[:-3] if bar.endswith("Utc") else bar

    unit = base[-1]
    value = int(base[:-1])

    multipliers = {"s": 1, "m": 60, "H": 3600, "D": 86400, "W": 604800, "M": 2592000}
    if unit not in multipliers:
        raise ValueError(f"不支持的时间粒度: {bar}")
    return value * multipliers[unit]
