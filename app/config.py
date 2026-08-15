"""配置模块 - 基于dataclass和dataclass_json的环境配置管理"""

import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

# 加载.env文件（如果存在）
load_dotenv()


@dataclass
class OKXConfig:
    """OKX API客户端配置"""
    api_key: str = field(default_factory=lambda: os.getenv("OKX_API_KEY", ""))
    secret_key: str = field(default_factory=lambda: os.getenv("OKX_SECRET_KEY", ""))
    passphrase: str = field(default_factory=lambda: os.getenv("OKX_PASSPHRASE", ""))
    sandbox: bool = field(
        default_factory=lambda: os.getenv("OKX_SANDBOX", "false").lower() == "true"
    )
    rate_limit_per_second: int = field(
        default_factory=lambda: int(os.getenv("OKX_RATE_LIMIT_PER_SECOND", "16"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("OKX_MAX_RETRIES", "5"))
    )
    retry_backoff: float = field(
        default_factory=lambda: float(os.getenv("OKX_RETRY_BACKOFF", "2.0"))
    )

    # ============ IP代理池配置（一个币一个IP并行下载） ============
    # 逗号分隔的代理URL列表（优先），如 http://127.0.0.1:7891,http://127.0.0.1:7892
    proxy_urls: str = field(
        default_factory=lambda: os.getenv("OKX_PROXY_URLS", "")
    )
    # 代理列表文件路径（备选）：每行一个代理URL，支持#注释
    proxy_list_file: str = field(
        default_factory=lambda: os.getenv("OKX_PROXY_LIST_FILE", "")
    )
    # Clash/Mihomo external-controller 地址（自动发现节点，仅1个出口IP时无加速效果）
    clash_api_url: str = field(
        default_factory=lambda: os.getenv("CLASH_API_URL", "")
    )
    # Clash 混合代理端口
    clash_proxy_url: str = field(
        default_factory=lambda: os.getenv("CLASH_PROXY_URL", "http://127.0.0.1:7890")
    )
    # 每个IP的请求限速（OKX按IP限频约20请求/2秒，实测8~10/s最稳，
    # 过高触发429；平滑限速器已消除突发，见 proxy_pool._TokenBucket。
    # 代理池模式下默认提到10，单IP/直连场景可通过 OKX_IP_RATE_LIMIT_PER_SECOND 覆盖）
    ip_rate_limit_per_second: int = field(
        default_factory=lambda: int(os.getenv("OKX_IP_RATE_LIMIT_PER_SECOND", "10"))
    )

    @property
    def base_url(self) -> str:
        """返回OKX公共API基础URL

        OKX目前没有独立的沙箱域名，实盘和模拟盘共用
        https://www.okx.com；沙箱模式通过鉴权配置区分。
        """
        return "https://www.okx.com"


@dataclass
class DatabaseConfig:
    """PostgreSQL + TimescaleDB 配置"""
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", "123456"))
    name: str = field(default_factory=lambda: os.getenv("DB_NAME", "okx_data"))
    schema: str = field(default_factory=lambda: os.getenv("DB_SCHEMA", "public"))
    pool_size: int = field(default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "10")))
    max_overflow: int = field(
        default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "40"))
    )
    pool_timeout: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_TIMEOUT", "30"))
    )

    # ============ Docker数据库引导（下载前自动检测并启动） ============
    # DB_USE_DOCKER: auto=数据库不可达时自动用Docker启动(默认)，true=同上，
    #                false=绝不动Docker(数据库不可达时直接报错)
    use_docker: str = field(
        default_factory=lambda: os.getenv("DB_USE_DOCKER", "auto")
    )
    # 容器名 / 镜像 / 数据卷（首次运行会创建并持久化数据到命名卷）
    container_name: str = field(
        default_factory=lambda: os.getenv("DB_CONTAINER_NAME", "okx-timescaledb")
    )
    image: str = field(
        default_factory=lambda: os.getenv(
            "DB_DOCKER_IMAGE", "timescale/timescaledb:latest-pg16")
    )
    data_volume: str = field(
        default_factory=lambda: os.getenv("DB_DOCKER_VOLUME", "okx-timescaledb-data")
    )

    @property
    def dsn(self) -> str:
        """返回SQLAlchemy连接字符串"""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class LoggingConfig:
    """日志系统配置"""
    level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    file_enabled: bool = field(
        default_factory=lambda: os.getenv("LOG_FILE_ENABLED", "true").lower() == "true"
    )
    max_bytes: int = field(
        default_factory=lambda: int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
    )
    backup_count: int = field(
        default_factory=lambda: int(os.getenv("LOG_BACKUP_COUNT", "5"))
    )


@dataclass
class DownloadConfig:
    """数据下载配置"""
    default_instrument: str = field(
        default_factory=lambda: os.getenv("DEFAULT_INSTRUMENT", "ETH-USDT-SWAP")
    )
    default_chain: str = field(
        default_factory=lambda: os.getenv("DEFAULT_CHAIN", "ETH-USDT")
    )
    default_bar: str = field(default_factory=lambda: os.getenv("DEFAULT_BAR", "1m"))
    
    # 支持的交易类型
    spot_instruments: List[str] = field(
        default_factory=lambda: ["BTC-USDT", "ETH-USDT"]
    )
    swap_instruments: List[str] = field(
        default_factory=lambda: ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    )

    # k线（K线接口可用的所有时间粒度）
    kline_bars: List[str] = field(
        default_factory=lambda: [
            "1m", "3m", "5m", "15m", "30m",
            "1H", "2H", "4H", "12H",
            "1D", "1W", "1M", "6HUtc", "1MUtc"
        ]
    )

    # K线接口每次请求的最大数量
    max_candles_per_request: int = 100

    # 资金费率（Funding rate）最大请求返回数量
    max_funding_rate_per_request: int = 100


@dataclass
class Config:
    """全局配置聚合"""
    okx: OKXConfig = field(default_factory=OKXConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)

    timezone: str = field(default_factory=lambda: os.getenv("TIMEZONE", "Asia/Shanghai"))
