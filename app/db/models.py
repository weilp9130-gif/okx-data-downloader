"""ORM数据模型 - SQLAlchemy

包含：
- Candle: K线数据
- FundingRate: 资金费率数据

所有表使用复合主键 + TimescaleDB hypertable按键。
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

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
        # 业务唯一键是 (inst_id, snapshot_at)；TimescaleDB 要求分区列 ts 必须
        # 包含在唯一约束中，因此主键为三列，去重语义仍由 (inst_id, snapshot_at) 决定
        PrimaryKeyConstraint("inst_id", "snapshot_at", "ts"),
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


class DataConflict(Base):
    """Raw 数据冲突登记（DATA_CONFLICT）

    相同业务唯一键但 payload hash 不同时登记，禁止静默覆盖，需人工确认。
    """

    __tablename__ = "data_conflicts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_name = Column(String(50), nullable=False)
    inst_id = Column(String(50), nullable=False)
    biz_key = Column(String(200), nullable=False)
    existing_hash = Column(String(64))
    incoming_hash = Column(String(64))
    existing_payload = Column(JSONB)
    incoming_payload = Column(JSONB)
    source = Column(String(20))
    detected_at = Column(DateTime(timezone=True))
    status = Column(String(20))

    __table_args__ = (
        Index("ix_data_conflicts_table_biz", "table_name", "inst_id", "biz_key"),
    )

    def __repr__(self) -> str:
        return (
            f"<DataConflict(table={self.table_name}, inst={self.inst_id}, "
            f"key={self.biz_key}, status={self.status})>"
        )


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


class LatencySample(Base):
    """延迟探针原始样本（latency_probe 专用，TimescaleDB hypertable）

    只存 raw 指标（ws_ping_rtt / http_rtt / raw_ws_receive_latency /
    strategy_*）；`corrected_ws_receive_latency` 禁止落表（P0），只存在于
    latency_summaries / 内存聚合 / 用户 SQL。
    `sample_ts` = 本地接收时刻（WS 为 recv 时刻，HTTP 为 REST 完成时刻 t1）；
    `session` = 0 表示非 WS/session-independent，>=1 为真实 WS session。
    """

    __tablename__ = "latency_samples"

    id = Column(BigInteger, primary_key=True, autoincrement=True)  # PK 首位
    sample_ts = Column(DateTime(timezone=True), primary_key=True)  # 分区键 (= recv_ts)
    session = Column(Integer, nullable=False)
    source = Column(String(20), nullable=False)
    inst_id = Column(String(50), nullable=False)      # '__system__' for __http__/__ws__
    channel = Column(String(32), nullable=False)      # trades/bbo-tbt/.../__ws__/__http__
    metric = Column(String(32), nullable=False)       # raw only
    value_ms = Column(Float, nullable=False)          # DOUBLE PRECISION
    exchange_ts = Column(DateTime(timezone=True))     # WS 行情 ts；rtt 类为 NULL
    recv_ts = Column(DateTime(timezone=True))         # 本地接收墙钟时刻
    clock_offset_ms = Column(Float)                   # 每样本 offset 快照（http 为诊断）

    __table_args__ = (
        Index("ix_latency_samples_inst_ts", "inst_id", "sample_ts"),
        Index("ix_latency_samples_channel_metric", "channel", "metric", "sample_ts"),
    )

    def __repr__(self) -> str:
        return (
            f"<LatencySample(inst={self.inst_id}, channel={self.channel}, "
            f"metric={self.metric}, value={self.value_ms}ms)>"
        )


class LatencySummary(Base):
    """延迟探针窗口汇总（普通表，内存聚合 UPSERT，禁 SQL 反查 samples）"""

    __tablename__ = "latency_summaries"

    window_start = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(20), nullable=False)
    inst_id = Column(String(50), nullable=False)
    channel = Column(String(32), nullable=False)
    metric = Column(String(32), nullable=False)       # 含 corrected_ws_receive_latency（派生层）
    n = Column(BigInteger, nullable=False)
    min_ms = Column(Float, nullable=False)
    mean_ms = Column(Float, nullable=False)
    p50_ms = Column(Float, nullable=False)
    p95_ms = Column(Float, nullable=False)
    p99_ms = Column(Float, nullable=False)
    max_ms = Column(Float, nullable=False)
    jitter_ms = Column(Float, nullable=False)         # 样本标准差；n<2 -> 0

    __table_args__ = (
        PrimaryKeyConstraint("window_start", "source", "channel", "metric", "inst_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<LatencySummary(window={self.window_start}, {self.channel}/"
            f"{self.metric}, n={self.n}, p99={self.p99_ms})>"
        )


class LatencyProbeStats(Base):
    """延迟探针系统级计数器（普通表，不按 inst/channel/session 拆分）"""

    __tablename__ = "latency_probe_stats"

    window_start = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(20), nullable=False)
    metric = Column(String(32), nullable=False)
    value = Column(BigInteger, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("window_start", "source", "metric"),
    )

    def __repr__(self) -> str:
        return f"<LatencyProbeStats(window={self.window_start}, {self.metric}={self.value})>"


# ====================================================================
# metadata schema：平台运营表（OKX Quant Platform）
# ====================================================================

METADATA_SCHEMA = "metadata"


class TaskJob(Base):
    """任务（jobs）：平台任务队列，Worker 通过 DB 原子认领执行

    状态机：PENDING→QUEUED→ASSIGNED→RUNNING→SUCCESS/FAILED/CANCELLED/INTERRUPTED。
    """

    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_status_created", "status", "created_at"),
        Index("ix_jobs_group", "group_id"),
        Index("ix_jobs_parent", "parent_job_id"),
        Index("ix_jobs_worker", "assigned_worker_id"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(UUID(as_uuid=True), primary_key=True)
    group_id = Column(UUID(as_uuid=True), nullable=True)
    task_no = Column(String(30), nullable=False)
    task_type = Column(String(30), nullable=False)
    params = Column(JSONB, nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    priority = Column(Integer, nullable=False, default=0)
    required_capability = Column(String(30), nullable=True)
    rate_group = Column(String(30), nullable=True)
    assigned_worker_id = Column(UUID(as_uuid=True), nullable=True)
    node = Column(String(50), nullable=True)
    pid = Column(Integer, nullable=True)
    exit_code = Column(Integer, nullable=True)
    attempt_no = Column(Integer, nullable=False, default=0)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retry = Column(Integer, nullable=False, default=0)
    parent_job_id = Column(UUID(as_uuid=True), nullable=True)
    depends_on_job_id = Column(UUID(as_uuid=True), nullable=True)
    workflow_id = Column(UUID(as_uuid=True), nullable=True)
    progress = Column(JSONB, nullable=True)
    heartbeat_at = Column(DateTime(timezone=True), nullable=True)
    cancel_requested = Column(Boolean, nullable=False, default=False)
    error = Column(String(2000), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return f"<TaskJob(task_no={self.task_no}, type={self.task_type}, status={self.status})>"


class JobAttempt(Base):
    """任务执行记录（job_attempts）：每次 attempt 的日志/进度分文件"""

    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_no", name="uq_attempt_job_no"),
        Index("ix_attempts_job", "job_id"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey(f"{METADATA_SCHEMA}.jobs.id"), nullable=False)
    attempt_no = Column(Integer, nullable=False)
    worker_id = Column(UUID(as_uuid=True), nullable=True)
    pid = Column(Integer, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    exit_code = Column(Integer, nullable=True)
    log_path = Column(String(500), nullable=True)
    progress_path = Column(String(500), nullable=True)
    error = Column(String(2000), nullable=True)

    def __repr__(self) -> str:
        return f"<JobAttempt(job={self.job_id}, no={self.attempt_no}, exit={self.exit_code})>"


class Worker(Base):
    """任务 Worker：独立进程注册行（capabilities 过滤认领）"""

    __tablename__ = "workers"
    __table_args__ = (
        Index("ix_workers_status", "status"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(50), nullable=False)
    node = Column(String(50), nullable=True)
    hostname = Column(String(100), nullable=True)
    ip = Column(String(50), nullable=True)
    python_version = Column(String(30), nullable=True)
    os = Column(String(100), nullable=True)
    worker_version = Column(String(20), nullable=True)
    capabilities = Column(JSONB, nullable=False, default=list)
    status = Column(String(20), nullable=False, default="IDLE")
    capacity = Column(Integer, nullable=False, default=1)
    last_heartbeat_at = Column(DateTime(timezone=True), nullable=True)
    current_task_count = Column(Integer, nullable=False, default=0)
    last_error = Column(String(2000), nullable=True)
    registered_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<Worker(name={self.name}, status={self.status})>"


class AuditLog(Base):
    """审计日志（audit_log）：平台管理操作记录"""

    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_ts", "ts"),
        Index("ix_audit_target", "target_type", "target_id"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts = Column(DateTime(timezone=True), nullable=False)
    actor = Column(String(50), nullable=False, default="local")
    action = Column(String(50), nullable=False)
    target_type = Column(String(50), nullable=True)
    target_id = Column(String(100), nullable=True)
    detail = Column(JSONB, nullable=True)

    def __repr__(self) -> str:
        return f"<AuditLog(ts={self.ts}, action={self.action}, target={self.target_type})>"


class DataAsset(Base):
    """数据资产（data_asset）：期望存在的数据集×交易对组合（无状态字段）"""

    __tablename__ = "data_asset"
    __table_args__ = (
        UniqueConstraint("exchange", "market", "inst_id", "dataset", "bar",
                         name="uq_asset_identity"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False, default="OKX")
    market = Column(String(20), nullable=False, default="SWAP")
    inst_id = Column(String(50), nullable=False)
    dataset = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return f"<DataAsset(inst={self.inst_id}, dataset={self.dataset}, bar={self.bar})>"


class DataAssetState(Base):
    """资产状态（data_asset_state）：动态计算结果，状态只存在于此表"""

    __tablename__ = "data_asset_state"
    __table_args__ = (
        Index("ix_asset_state_asset", "asset_id"),
        Index("ix_asset_state_status", "status"),
        {"schema": METADATA_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    asset_id = Column(BigInteger, ForeignKey(f"{METADATA_SCHEMA}.data_asset.id"), nullable=False)
    earliest_ts = Column(DateTime(timezone=True), nullable=True)
    latest_ts = Column(DateTime(timezone=True), nullable=True)
    row_count = Column(BigInteger, nullable=False, default=0)
    expected_rows = Column(BigInteger, nullable=True)
    missing_rows = Column(BigInteger, nullable=True)
    duplicates = Column(BigInteger, nullable=True)
    invalid_rows = Column(BigInteger, nullable=True)
    quality_score = Column(Numeric(5, 1), nullable=True)
    freshness_lag_sec = Column(Numeric(20, 4), nullable=True)
    status = Column(String(20), nullable=False, default="NO_DATA")
    checked_at = Column(DateTime(timezone=True), nullable=True)
    full_recount_at = Column(DateTime(timezone=True), nullable=True)
    last_check_at = Column(DateTime(timezone=True), nullable=True)
    detail = Column(JSONB, nullable=True)

    def __repr__(self) -> str:
        return f"<DataAssetState(asset={self.asset_id}, status={self.status}, rows={self.row_count})>"


class DatasetDefinition(Base):
    """数据集定义（dataset_definition）：「应该有什么数据」，质量/资产引导基准"""

    __tablename__ = "dataset_definition"
    __table_args__ = (
        PrimaryKeyConstraint("dataset", "bar", "version"),
        {"schema": METADATA_SCHEMA},
    )

    dataset = Column(String(50), nullable=False)
    bar = Column(String(10), nullable=False, default="")
    version = Column(String(10), nullable=False, default="v1")
    table_name = Column(String(50), nullable=False)
    primary_time_column = Column(String(30), nullable=False, default="ts")
    source = Column(String(20), nullable=True)
    interval_seconds = Column(Integer, nullable=True)
    expected_freshness_sec = Column(Integer, nullable=True)
    retention_days = Column(Integer, nullable=False, default=0)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return f"<DatasetDefinition(dataset={self.dataset}, bar={self.bar}, table={self.table_name})>"
