"""数据库模块：连接、ORM 模型与同步状态"""

from .database import (
    Base,
    dispose_engine,
    get_engine,
    get_session,
    init_db,
    session_scope,
)
from .db_docker import ensure_database

_MODEL_NAMES = (
    "Candle",
    "FundingRate",
    "DownloadState",
    "Trade",
    "OrderBookSnapshot",
    "OrderBookFactor",
    "OrderBookSyncState",
    "TradeAggregate",
    "TradesSyncState",
    "OpenInterest",
    "OpenInterestRealtime",
    "MarkPrice",
    "IndexPrice",
    "OISyncState",
    "MarkPriceSyncState",
    "IndexPriceSyncState",
    "FundingSyncState",
    "DataQualityState",
    "MarketDataProvenance",
    "Instrument",
    "DataConflict",
    "RecoveryEvent",
    "DataGap",
    "LatencySample",
    "LatencySummary",
    "LatencyProbeStats",
)


def __getattr__(name):
    """惰性 re-export 模型：engine-only 消费者无需加载全部 ORM 模型"""
    if name in _MODEL_NAMES:
        from . import models

        return getattr(models, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Base",
    "dispose_engine",
    "get_engine",
    "get_session",
    "init_db",
    "session_scope",
    "ensure_database",
    *_MODEL_NAMES,
]
