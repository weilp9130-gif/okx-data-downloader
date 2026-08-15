"""日志系统模块

统一日志规则，保证所有入口脚本输出清晰、直观、可检索：

## 日志格式
    {时间} {级别} | {组件} | {消息}
    文件: 2026-08-15 14:17:23,123 INFO  | downloader.candles | [进度] AVAX-USDT-SWAP | 1m | 回溯至 2023-04-25 05:20 | 页 400
    控制台: 14:17:23 INFO  | download_all | ===== 阶段1 全量同步 =====
- 组件名 = 模块名去掉 app. 前缀（如 app.downloader.candles -> downloader.candles）；
  入口脚本为脚本文件名（main / download_all / sync_continuous / sync_daemon）。

## 日志级别规则
- DEBUG   : 底层细节（单请求参数/响应、SQL、调试信息）
- INFO    : 正常进度（任务开始/完成、阶段切换、轮次汇总、定期进度）
- WARNING : 可恢复的瞬时问题（单次重试、换代理、单合约失败、单次429）
- ERROR   : 持久失败、异常导致功能中断

## 日志文件规则
- 每个入口脚本一个独立日志文件：logs/{脚本名}_{YYYYMMDD}.log
  （main / download_all / sync_continuous / sync_daemon），避免多进程互相穿插。
- 单文件最大 10MB，轮转保留 5 个备份；文件 UTF-8 编码。
- 第三方库降噪：urllib3=ERROR、sqlalchemy=WARNING。

用法：
    from app.utils.logger import setup_logging, get_logger
    setup_logging(name="download_all", level="INFO")
    logger = get_logger(__name__)   # 组件名自动取模块短名
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

# 项目根目录（app/utils/logger.py -> 项目根）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"

# 日志格式（组件名由 _ComponentFormatter 注入）
CONSOLE_FORMAT = "%(asctime)s %(levelname)-5s | %(component)s | %(message)s"
FILE_FORMAT = "%(asctime)s %(levelname)-5s | %(component)s | %(message)s"
CONSOLE_DATEFMT = "%H:%M:%S"           # 控制台只看时间，不用日期
FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"     # 文件带日期（Formatter默认带毫秒）

# 第三方库日志降噪级别
_THIRD_PARTY_LEVELS = {
    "urllib3": logging.ERROR,
    "requests": logging.ERROR,
    "sqlalchemy": logging.WARNING,
    "okx": logging.WARNING,
}

# 全局状态标记
_initialized = False
_current_name = None
_current_level = None


def _short_name(name: str) -> str:
    """把 logger 名缩短为易读的组件名：
    - __main__ -> 入口脚本文件名（如 download_all）
    - app.downloader.candles -> downloader.candles
    """
    if name == "__main__":
        try:
            return Path(sys.argv[0]).stem if sys.argv and sys.argv[0] else "app"
        except Exception:
            return "app"
    if name.startswith("app."):
        return name[len("app."):]
    return name


class _ComponentFormatter(logging.Formatter):
    """格式化器：动态注入 component 字段"""

    def format(self, record: logging.LogRecord) -> str:
        record.component = _short_name(record.name)
        return super().format(record)


def setup_logging(
    name: str = "app",
    level: str = "INFO",
    log_dir: Path = LOG_DIR,
    file_enabled: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """初始化全局日志系统（进程内只生效一次）

    Args:
        name: 日志文件前缀（入口脚本名），生成 logs/{name}_{YYYYMMDD}.log
        level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_dir: 日志文件保存目录
        file_enabled: 是否启用文件日志
        max_bytes: 单个日志文件最大字节数
        backup_count: 保留的历史日志文件个数
    """
    global _initialized, _current_name, _current_level

    log_level = getattr(logging, level.upper(), logging.INFO)

    # 幂等：同一名称+级别只初始化一次；名称/级别变化时重建（如入口脚本
    # 用 setup_logging(name="download_all") 覆盖 import 阶段的控制台兜底）
    if _initialized and _current_name == name and _current_level == log_level:
        return

    # 根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除已有的handlers，避免重复
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 格式化器（控制台简洁、文件完整）
    console_formatter = _ComponentFormatter(CONSOLE_FORMAT, datefmt=CONSOLE_DATEFMT)
    file_formatter = _ComponentFormatter(FILE_FORMAT, datefmt=FILE_DATEFMT)

    # 控制台处理器（Windows下强制UTF-8，避免中文/emoji编码错误）
    console_handler = logging.StreamHandler(sys.stdout)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # 文件处理器（按入口脚本分文件 + 轮转）
    if file_enabled:
        log_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f"{name}_{date_str}.log"

        file_handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
            mode="a",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # 第三方库降噪
    for lib, lvl in _THIRD_PARTY_LEVELS.items():
        logging.getLogger(lib).setLevel(lvl)

    _initialized = True
    _current_name = name
    _current_level = log_level


def get_logger(name: str = None) -> logging.Logger:
    """获取一个logger实例

    若进程内尚未显式 setup_logging()，会先用"仅控制台"兜底初始化
    （不创建日志文件，避免入口脚本 import 阶段生成无意义的 app_*.log）；
    入口脚本的 main() 里应显式调用 setup_logging(name=脚本名, ...)。

    Args:
        name: logger名称，通常传模块名 __name__

    Returns:
        logging.Logger
    """
    if not _initialized:
        setup_logging(file_enabled=False)
    return logging.getLogger(name or __name__)
