"""日志系统模块

提供统一的日志初始化和获取机制，支持控制台 + 文件轮转日志。
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

# 项目根目录（app/utils/logger.py -> 项目根）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"

# 日志格式
CONSOLE_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
FILE_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)

# 全局状态标记
_initialized = False


def setup_logging(
    level: str = "INFO",
    log_dir: Path = LOG_DIR,
    file_enabled: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """初始化全局日志系统

    Args:
        level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_dir: 日志文件保存目录
        file_enabled: 是否启用文件日志
        max_bytes: 单个日志文件最大字节数
        backup_count: 保留的历史日志文件个数
    """
    global _initialized
    if _initialized:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)

    # 根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除已有的handlers，避免重复
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 格式化器
    console_formatter = logging.Formatter(CONSOLE_FORMAT)
    file_formatter = logging.Formatter(FILE_FORMAT)

    # 控制台处理器（Windows下强制UTF-8，避免emoji等报编码错）
    console_handler = logging.StreamHandler(sys.stdout)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # 文件处理器（轮转日志）
    if file_enabled:
        log_dir.mkdir(parents=True, exist_ok=True)

        # 生成按日期命名的日志文件
        date_str = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f"app_{date_str}.log"

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

    # 降噪第三方库日志
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("okx").setLevel(logging.WARNING)

    _initialized = True


def get_logger(name: str = None) -> logging.Logger:
    """获取一个logger实例

    Args:
        name: logger名称，通常传模块名 __name__

    Returns:
        logging.Logger
    """
    if not _initialized:
        setup_logging()
    return logging.getLogger(name or __name__)
