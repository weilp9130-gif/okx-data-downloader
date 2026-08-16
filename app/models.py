"""ORM数据模型 - SQLAlchemy

包含：
- Candle: K线数据
- FundingRate: 资金费率数据

所有表使用复合主键 + TimescaleDB hypertable按键。
"""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB

from .database import Base


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


class DownloadState(Base):
    """下载验证水位线：记录 (inst_id, bar) 已确认完整到的时间点

    缺失窗口检测按天比对条数时，会跳过 verified_upto 之前的区间，
    避免每次运行都重扫整段历史。
    """

    __tablename__ = "download_state"

    inst_id = Column(String(50), primary_key=True)
    bar = Column(String(10), primary_key=True)
    verified_upto = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return (
            f"<DownloadState(inst={self.inst_id}, bar={self.bar}, "
            f"verified_upto={self.verified_upto})>"
        )


class Trade(Base):
    """成交明细（Raw Exchange Data）

    业务唯一键经真实 API 验证为 (inst_id, trade_id)。
    OKX SWAP/FUTURES 的 sz 单位为合约张数，非 BTC/USDT 数量。
    """

    __tablename__ = "trades"

    inst_id = Column(String(50), nullable=False)
    trade_id = Column(String(80), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    px = Column(Numeric(20, 8), nullable=False)
    sz = Column(Numeric(30, 16), nullable=False)
    side = Column(String(10), nullable=False)
    source = Column(String(20))
    received_at = Column(DateTime(timezone=True))
    fetched_at = Column(DateTime(timezone=True))
    ingested_at = Column(DateTime(timezone=True), nullable=False)
    fill_time = Column(DateTime(timezone=True))
    raw_json = Column(JSONB)
    raw_hash = Column(String(64))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "trade_id", "ts"),
        Index("ix_trades_inst_ts", "inst_id", "ts"),
        Index("ix_trades_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<Trade(inst={self.inst_id}, trade_id={self.trade_id}, ts={self.ts})>"


class OrderBookSnapshot(Base):
    """本地重建 OrderBook 采样快照（Derived / Reconstructed）

    注意：这不是 Raw Exchange Data，而是本地重建盘口在固定采样点的状态。
    """

    __tablename__ = "order_book_snapshots"

    inst_id = Column(String(50), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    exchange_ts = Column(DateTime(timezone=True), nullable=False)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True))
    fetched_at = Column(DateTime(timezone=True))
    bids = Column(JSONB)
    asks = Column(JSONB)
    best_bid_px = Column(Numeric(20, 8))
    best_bid_sz = Column(Numeric(30, 16))
    best_ask_px = Column(Numeric(20, 8))
    best_ask_sz = Column(Numeric(30, 16))
    seq_id = Column(BigInteger)
    prev_seq_id = Column(BigInteger)
    checksum = Column(Integer)
    source = Column(String(20))
    snapshot_type = Column(String(20))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "snapshot_at"),
        Index("ix_ob_inst_snapshot", "inst_id", "snapshot_at"),
        Index("ix_ob_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<OrderBookSnapshot(inst={self.inst_id}, ts={self.ts}, snapshot_at={self.snapshot_at})>"


class OrderBookFactor(Base):
    """OrderBook 因子（spread / mid / wmid / imbalance 等）"""

    __tablename__ = "order_book_factors"

    inst_id = Column(String(50), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    spread = Column(Numeric(20, 8))
    mid = Column(Numeric(20, 8))
    wmid = Column(Numeric(20, 8))
    bid_depth_5 = Column(Numeric(30, 16))
    ask_depth_5 = Column(Numeric(30, 16))
    bid_depth_10 = Column(Numeric(30, 16))
    ask_depth_10 = Column(Numeric(30, 16))
    imbalance_5 = Column(Numeric(20, 8))
    imbalance_10 = Column(Numeric(20, 8))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "ts"),
        Index("ix_obf_inst_ts", "inst_id", "ts"),
    )

    def __repr__(self) -> str:
        return f"<OrderBookFactor(inst={self.inst_id}, ts={self.ts})>"


class OrderBookSyncState(Base):
    """OrderBook 同步状态"""

    __tablename__ = "order_book_sync_state"

    inst_id = Column(String(50), primary_key=True)
    prev_seq = Column(BigInteger)
    latest_seq = Column(BigInteger)
    checksum = Column(Integer)
    latest_ts = Column(DateTime(timezone=True))
    resync_count = Column(Integer, default=0)
    last_resync_reason = Column(String(40))
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"<OrderBookSyncState(inst={self.inst_id}, status={self.status})>"


class TradeAggregate(Base):
    """Trade 聚合数据（1s / 1m 等时间桶）

    由 trades 按时间桶聚合得到，支持迟到数据重新计算。
    """

    __tablename__ = "trade_aggregates"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    o = Column(Numeric(20, 8))
    h = Column(Numeric(20, 8))
    l = Column(Numeric(20, 8))
    c = Column(Numeric(20, 8))
    vol = Column(Numeric(30, 16))
    vol_buy = Column(Numeric(30, 16))
    vol_sell = Column(Numeric(30, 16))
    vol_contract = Column(Numeric(30, 16))
    cnt = Column(Integer)
    cnt_buy = Column(Integer)
    cnt_sell = Column(Integer)
    is_final = Column(Numeric(1, 0), default=0)
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar", "ts"),
        Index("ix_trade_agg_inst_ts", "inst_id", "ts"),
        Index("ix_trade_agg_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<TradeAggregate(inst={self.inst_id}, bar={self.bar}, ts={self.ts}, vol={self.vol})>"


class TradesSyncState(Base):
    """Trades 同步状态"""

    __tablename__ = "trades_sync_state"

    inst_id = Column(String(50), primary_key=True)
    latest_trade_id = Column(String(80))
    latest_ts = Column(DateTime(timezone=True))
    status = Column(String(20))
    recovery_count = Column(Integer, default=0)
    last_recovery_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    updated_at = Column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"<TradesSyncState(inst={self.inst_id}, status={self.status})>"


class OpenInterest(Base):
    """持仓量（Open Interest）

    注意：OKX 当前仅提供 `/api/v5/public/open-interest` 单点快照接口，
    无历史 OI 查询端点。本表用于聚合该快照的时序数据，bar 通常为
    采集周期或统一标记（如 "current"）。
    """

    __tablename__ = "open_interest"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    oi = Column(Numeric(30, 16), nullable=False)
    oi_ccy = Column(Numeric(30, 16))
    oi_usd = Column(Numeric(30, 16))
    raw_json = Column(JSONB)
    ingested_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar", "ts"),
        Index("ix_oi_inst_ts", "inst_id", "ts"),
        Index("ix_oi_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<OpenInterest(inst={self.inst_id}, bar={self.bar}, ts={self.ts}, oi={self.oi})>"


class OpenInterestRealtime(Base):
    """WebSocket 实时持仓量观测值

    Phase 7 创建：由 WS open-interest 频道写入，语义为实时观测值，
    非 REST 历史 OI 的另一种存储。
    """

    __tablename__ = "open_interest_realtime"

    inst_id = Column(String(50), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    oi = Column(Numeric(30, 16))
    oi_ccy = Column(Numeric(30, 16))
    oi_usd = Column(Numeric(30, 16))
    raw_json = Column(JSONB)
    received_at = Column(DateTime(timezone=True))
    ingested_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "ts"),
        Index("ix_oir_inst_ts", "inst_id", "ts"),
        Index("ix_oir_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<OpenInterestRealtime(inst={self.inst_id}, ts={self.ts}, oi={self.oi})>"


class MarkPrice(Base):
    """标记价格 K线"""

    __tablename__ = "mark_prices"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    o = Column(Numeric(20, 8), nullable=False)
    h = Column(Numeric(20, 8), nullable=False)
    l = Column(Numeric(20, 8), nullable=False)
    c = Column(Numeric(20, 8), nullable=False)
    confirm = Column(String(10))
    source = Column(String(20))
    received_at = Column(DateTime(timezone=True))
    fetched_at = Column(DateTime(timezone=True))
    raw_json = Column(JSONB)
    ingested_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar", "ts"),
        Index("ix_mp_inst_ts", "inst_id", "ts"),
        Index("ix_mp_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<MarkPrice(inst={self.inst_id}, bar={self.bar}, ts={self.ts}, c={self.c})>"


class IndexPrice(Base):
    """指数价格 K线"""

    __tablename__ = "index_prices"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    o = Column(Numeric(20, 8), nullable=False)
    h = Column(Numeric(20, 8), nullable=False)
    l = Column(Numeric(20, 8), nullable=False)
    c = Column(Numeric(20, 8), nullable=False)
    confirm = Column(String(10))
    source = Column(String(20))
    received_at = Column(DateTime(timezone=True))
    fetched_at = Column(DateTime(timezone=True))
    raw_json = Column(JSONB)
    ingested_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar", "ts"),
        Index("ix_ip_inst_ts", "inst_id", "ts"),
        Index("ix_ip_ts", "ts"),
    )

    def __repr__(self) -> str:
        return f"<IndexPrice(inst={self.inst_id}, bar={self.bar}, ts={self.ts}, c={self.c})>"


class OISyncState(Base):
    """Open Interest 同步状态"""

    __tablename__ = "oi_sync_state"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    earliest_ts = Column(DateTime(timezone=True))
    latest_ts = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar"),
    )

    def __repr__(self) -> str:
        return f"<OISyncState(inst={self.inst_id}, bar={self.bar}, status={self.status})>"


class MarkPriceSyncState(Base):
    """Mark Price 同步状态"""

    __tablename__ = "mark_price_sync_state"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    earliest_ts = Column(DateTime(timezone=True))
    latest_ts = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar"),
    )

    def __repr__(self) -> str:
        return f"<MarkPriceSyncState(inst={self.inst_id}, bar={self.bar}, status={self.status})>"


class IndexPriceSyncState(Base):
    """Index Price 同步状态"""

    __tablename__ = "index_price_sync_state"

    inst_id = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False)
    earliest_ts = Column(DateTime(timezone=True))
    latest_ts = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("inst_id", "bar"),
    )

    def __repr__(self) -> str:
        return f"<IndexPriceSyncState(inst={self.inst_id}, bar={self.bar}, status={self.status})>"


class FundingSyncState(Base):
    """Funding Rate 同步状态"""

    __tablename__ = "funding_sync_state"

    inst_id = Column(String(50), primary_key=True)
    earliest_ts = Column(DateTime(timezone=True))
    latest_ts = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"<FundingSyncState(inst={self.inst_id}, status={self.status})>"


class DataQualityState(Base):
    """数据质量状态"""

    __tablename__ = "data_quality_state"

    data_type = Column(String(50), nullable=False)
    inst_id = Column(String(50), nullable=False)
    latest_ts = Column(DateTime(timezone=True))
    expected_ts = Column(DateTime(timezone=True))
    gap_seconds = Column(Numeric(20, 4))
    last_success_at = Column(DateTime(timezone=True))
    last_error_at = Column(DateTime(timezone=True))
    error_count = Column(Integer, default=0)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        PrimaryKeyConstraint("data_type", "inst_id"),
    )

    def __repr__(self) -> str:
        return f"<DataQualityState(type={self.data_type}, inst={self.inst_id}, status={self.status})>"


class MarketDataProvenance(Base):
    """市场数据来源追踪（用于 funding_rates 等旧表，因其 schema 不可修改）"""

    __tablename__ = "market_data_provenance"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    data_type = Column(String(50), nullable=False)
    inst_id = Column(String(50), nullable=False)
    biz_key = Column(String(200), nullable=False)
    source = Column(String(20), nullable=False)
    received_at = Column(DateTime(timezone=True))
    ingested_at = Column(DateTime(timezone=True))
    raw_hash = Column(String(64))
    created_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_prov_type_biz", "data_type", "inst_id", "biz_key"),
    )

    def __repr__(self) -> str:
        return f"<MarketDataProvenance(type={self.data_type}, inst={self.inst_id}, key={self.biz_key})>"


class Instrument(Base):
    """交易对/产品信息（Instruments）

    来自 OKX /api/v5/public/instruments 接口，是后续所有 Downloader
    的元数据依赖（ctVal/tickSz/lotSz 等）。
    """

    __tablename__ = "instruments"

    inst_id = Column(String(50), primary_key=True)
    inst_type = Column(String(20), nullable=False)
    base_ccy = Column(String(20))
    quote_ccy = Column(String(20))
    settle_ccy = Column(String(20))
    ct_val = Column(Numeric(30, 16))
    ct_mult = Column(Numeric(30, 16))
    tick_sz = Column(Numeric(30, 16))
    lot_sz = Column(Numeric(30, 16))
    min_sz = Column(Numeric(30, 16))
    state = Column(String(20))
    list_time = Column(DateTime(timezone=True))
    exp_time = Column(DateTime(timezone=True))
    lever = Column(Numeric(10, 6))
    max_lmt_sz = Column(Numeric(30, 16))
    max_mkt_sz = Column(Numeric(30, 16))
    raw_json = Column(JSONB)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_instruments_type_state", "inst_type", "state"),
        Index("ix_instruments_list_time", "list_time"),
    )

    def __repr__(self) -> str:
        return f"<Instrument(inst_id={self.inst_id}, type={self.inst_type}, state={self.state})>"


class RecoveryEvent(Base):
    """Recovery 事件表（Phase 8 创建）

    用于把日志、数据库、程序日志关联到同一次 Recovery Operation。
    """

    __tablename__ = "recovery_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    recovery_id = Column(String(36), nullable=False)
    data_type = Column(String(50), nullable=False)
    inst_id = Column(String(50), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    reason = Column(String(40))
    from_ts = Column(DateTime(timezone=True))
    to_ts = Column(DateTime(timezone=True))
    from_id = Column(String(80))
    to_id = Column(String(80))
    rows_recovered = Column(BigInteger)
    status = Column(String(20))
    error_message = Column(String(2000))
    updated_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_recovery_events_type_inst", "data_type", "inst_id", "started_at"),
    )

    def __repr__(self) -> str:
        return f"<RecoveryEvent(type={self.data_type}, inst={self.inst_id}, status={self.status})>"


class DataGap(Base):
    """数据缺口登记表（Phase 8 创建）

    同一 data_type + inst_id + gap interval 只登记一次。
    """

    __tablename__ = "data_gaps"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    data_type = Column(String(50), nullable=False)
    inst_id = Column(String(50), nullable=False)
    start_ts = Column(DateTime(timezone=True))
    end_ts = Column(DateTime(timezone=True))
    gap_type = Column(String(40))
    status = Column(String(20))
    detected_at = Column(DateTime(timezone=True))
    recovered_at = Column(DateTime(timezone=True))
    recovery_rows = Column(BigInteger)
    error_message = Column(String(2000))

    __table_args__ = (
        Index("ix_data_gaps_type_inst", "data_type", "inst_id", "start_ts"),
    )

    def __repr__(self) -> str:
        return f"<DataGap(type={self.data_type}, inst={self.inst_id}, status={self.status})>"
