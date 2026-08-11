"""ORM数据模型 - SQLAlchemy

包含：
- Candle: K线数据
- FundingRate: 资金费率数据

所有表使用复合主键 + TimescaleDB hypertable按键。
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
    DateTime,
    Numeric,
    PrimaryKeyConstraint,
    Index,
)

from database import Base


class Candle(Base):
    """K线数据模型"""

    __tablename__ = "candles"

    # 复合主键：instId + bar + ts
    inst_id = Column(String(50), nullable=False)          # 产品ID，如 ETH-USDT-SWAP
    bar = Column(String(10), nullable=False)              # 时间粒度，如 1m, 1H
    ts = Column(DateTime(timezone=True), nullable=False)  # 时间戳（UTC）
    o = Column(Numeric(20, 8), nullable=False)            # 开盘价
    h = Column(Numeric(20, 8), nullable=False)            # 最高价
    l = Column(Numeric(20, 8), nullable=False)            # 最低价
    c = Column(Numeric(20, 8), nullable=False)            # 收盘价
    vol = Column(Numeric(30, 8), nullable=False)          # 成交量
    vol_ccy = Column(Numeric(30, 8), nullable=True)       # 计价量（折算币种）
    vol_ccy_quote = Column(Numeric(30, 8), nullable=True) # 计价量（报价币种）
    confirm = Column(String(10), nullable=True)           # K线状态（'0'未完成，'1'已确定）

    # 复合主键
    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar", "ts"),
        Index("ix_candles_ts", "ts"),
        Index("ix_candles_inst_ts", "inst_id", "ts"),
    )

    def __repr__(self) -> str:
        return (
            f"<Candle(inst={self.inst_id}, bar={self.bar}, "
            f"ts={self.ts}, o={self.o}, c={self.c})>"
        )

    def to_dict(self) -> dict:
        """转换为dict"""
        return {
            "inst_id": self.inst_id,
            "bar": self.bar,
            "ts": self.ts,
            "o": self.o,
            "h": self.h,
            "l": self.l,
            "c": self.c,
            "vol": self.vol,
            "vol_ccy": self.vol_ccy,
            "vol_ccy_quote": self.vol_ccy_quote,
            "confirm": self.confirm,
        }


class FundingRate(Base):
    """资金费率数据模型（合约产品）"""

    __tablename__ = "funding_rates"

    inst_id = Column(String(50), nullable=False)          # 合约ID，如 ETH-USDT-SWAP
    ts = Column(DateTime(timezone=True), nullable=False)  # 资金费率时间点（UTC）
    funding_rate = Column(Numeric(20, 8), nullable=False) # 资金费率
    realized_rate = Column(Numeric(20, 8), nullable=True) # 已实现资金费率
    funding_time = Column(DateTime(timezone=True), nullable=True) # 实际结算时间

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "ts"),
        Index("ix_funding_ts", "ts"),
        Index("ix_funding_inst_ts", "inst_id", "ts"),
    )

    def __repr__(self) -> str:
        return (
            f"<FundingRate(inst={self.inst_id}, ts={self.ts}, "
            f"rate={self.funding_rate})>"
        )

    def to_dict(self) -> dict:
        return {
            "inst_id": self.inst_id,
            "ts": self.ts,
            "funding_rate": self.funding_rate,
            "realized_rate": self.realized_rate,
            "funding_time": self.funding_time,
        }
