"""数据库连接模块 - PostgreSQL + TimescaleDB

提供：
- 单例的SQLAlchemy Engine
- Session工厂
- 连接管理
- 表创建（含TimescaleDB hypertable）
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base

from .config import Config
from .utils.logger import get_logger

logger = get_logger(__name__)

# 声明式模型基类
Base = declarative_base()

# 全局Engine / Session
_engine: Engine = None
_SessionLocal: sessionmaker = None


def get_engine() -> Engine:
    """获取（或初始化）数据库Engine（单例）

    Returns:
        sqlalchemy.engine.Engine
    """
    global _engine
    if _engine is None:
        # 下载前确保数据库可用：已有数据库直连；不可达且允许时自动用Docker启动
        from .db_docker import ensure_database
        ensure_database()

        cfg = Config().database
        logger.info(f"连接数据库: {cfg.host}:{cfg.port}/{cfg.name}")

        _engine = create_engine(
            cfg.dsn,
            pool_size=cfg.pool_size,
            max_overflow=cfg.max_overflow,
            pool_timeout=cfg.pool_timeout,
            pool_pre_ping=True,          # 自动检测失效连接
            echo=False,
            future=True,
        )

        # 连接重连缓存
        @event.listens_for(_engine, "connect")
        def set_sql_mode(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.close()

        # 确保schema存在
        with _engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{cfg.schema}"'))
            conn.commit()

    return _engine


def get_session() -> Session:
    """获取一个新的数据库Session

    Returns:
        sqlalchemy.orm.Session
    """
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _SessionLocal()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """上下文管理器：自动提交/回滚/关闭Session

    Usage:
        with session_scope() as session:
            session.add(some_object)
            # 自动commit
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """初始化数据库：创建所有表和TimescaleDB hypertable"""
    from .models import (  # 导入以注册模型
        Candle,
        FundingRate,
        IndexPrice,
        Instrument,
        MarkPrice,
        MarketDataProvenance,
        OpenInterest,
        Trade,
        FundingSyncState,
        IndexPriceSyncState,
        MarkPriceSyncState,
        OISyncState,
        TradesSyncState,
    )

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    logger.info("数据库表结构创建完成")

    # 创建TimescaleDB hypertable
    _create_hypertables(engine)


def _create_hypertables(engine: Engine) -> None:
    """将特定表转换为TimescaleDB hypertable

    如果TimescaleDB扩展可用，将 candles 和 funding_rates 表
    转换为按时间分区的hypertable，极大提升时序查询性能。
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            conn.commit()

            # 将candles表转换为hypertable（分区键为ts列）
            conn.execute(text(
                "SELECT create_hypertable('candles', 'ts', "
                "chunk_time_interval => INTERVAL '2 weeks', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            # 将funding_rates表转换为hypertable（分区键为ts列）
            conn.execute(text(
                "SELECT create_hypertable('funding_rates', 'ts', "
                "chunk_time_interval => INTERVAL '2 weeks', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            # Phase 2 新增 hypertable
            conn.execute(text(
                "SELECT create_hypertable('open_interest', 'ts', "
                "chunk_time_interval => INTERVAL '2 weeks', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            conn.execute(text(
                "SELECT create_hypertable('mark_prices', 'ts', "
                "chunk_time_interval => INTERVAL '2 weeks', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            conn.execute(text(
                "SELECT create_hypertable('index_prices', 'ts', "
                "chunk_time_interval => INTERVAL '2 weeks', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            conn.execute(text(
                "SELECT create_hypertable('trades', 'ts', "
                "chunk_time_interval => INTERVAL '1 week', "
                "if_not_exists => TRUE, migrate_data => TRUE)"
            ))
            conn.commit()
            logger.info("TimescaleDB hypertable 创建完成")
    except Exception as e:
        logger.warning(f"TimescaleDB hypertable创建失败（可忽略，将使用普通表）: {e}")


def dispose_engine() -> None:
    """关闭数据库连接池"""
    global _engine, _SessionLocal
    if _engine:
        _engine.dispose()
        _engine = None
        _SessionLocal = None
        logger.info("数据库连接已关闭")
