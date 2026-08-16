"""WebSocket 断线恢复模块

Phase 8：统一 Recovery

- Recovery 事件记录到 recovery_events
- 数据缺口登记到 data_gaps（防重复）
- Trades: ID-based（REST history-trades 回补）
- OI/Mark/Index/Funding: Time-based（REST 回补）
- OrderBook: Seq Gap（等待新 snapshot，由 OrderBookHandler 状态机处理）
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import text

from ..database import get_engine
from ..downloader.funding import FundingRateDownloader
from ..downloader.index_price import IndexPriceDownloader
from ..downloader.mark_price import MarkPriceDownloader
from ..downloader.open_interest import OpenInterestDownloader
from ..downloader.trades import TradesDownloader
from ..okx_client import OKXClient
from ..utils.logger import get_logger

logger = get_logger(__name__)

RECOVERY_STATUS_RECOVERED = "RECOVERED"
RECOVERY_STATUS_PARTIAL = "PARTIAL_RECOVERED"
RECOVERY_STATUS_UNRECOVERABLE = "UNRECOVERABLE"

GAP_STATUS_OPEN = "OPEN"
GAP_STATUS_RECOVERED = "RECOVERED"
GAP_STATUS_UNRECOVERABLE = "UNRECOVERABLE"


class RecoveryEventStore:
    """Recovery 事件持久化"""

    def __init__(self):
        self.engine = get_engine()

    def start(self, data_type: str, inst_id: str, reason: str = None,
              from_ts=None, to_ts=None, from_id=None, to_id=None) -> str:
        """创建一条 recovery 事件，返回 recovery_id"""
        recovery_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO recovery_events
                        (recovery_id, data_type, inst_id, started_at, reason,
                         from_ts, to_ts, from_id, to_id, status, updated_at)
                    VALUES
                        (:rid, :data_type, :inst_id, :now, :reason,
                         :from_ts, :to_ts, :from_id, :to_id, 'RUNNING', :now)
                    """
                ),
                {
                    "rid": recovery_id,
                    "data_type": data_type,
                    "inst_id": inst_id,
                    "now": now,
                    "reason": reason,
                    "from_ts": from_ts,
                    "to_ts": to_ts,
                    "from_id": from_id,
                    "to_id": to_id,
                },
            )
        return recovery_id

    def finish(self, recovery_id: str, status: str, rows_recovered: int,
               error_message: str = None, to_ts=None, to_id=None) -> None:
        """结束一条 recovery 事件"""
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE recovery_events
                    SET finished_at = :now, status = :status,
                        rows_recovered = :rows, error_message = :err,
                        to_ts = COALESCE(:to_ts, to_ts),
                        to_id = COALESCE(:to_id, to_id),
                        updated_at = :now
                    WHERE recovery_id = :rid
                    """
                ),
                {
                    "rid": recovery_id,
                    "now": now,
                    "status": status,
                    "rows": rows_recovered,
                    "err": error_message,
                    "to_ts": to_ts,
                    "to_id": to_id,
                },
            )


class DataGapStore:
    """数据缺口登记（防重复）"""

    def __init__(self):
        self.engine = get_engine()

    def register_open(self, data_type: str, inst_id: str, start_ts, end_ts,
                      gap_type: str, error_message: str = None) -> bool:
        """登记 OPEN 缺口；已存在同类型同区间 OPEN 缺口则跳过

        Returns:
            bool: True if inserted, False if already exists
        """
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            exists = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM data_gaps
                    WHERE data_type = :data_type AND inst_id = :inst_id
                      AND status = 'OPEN'
                      AND COALESCE(start_ts, 'epoch') = COALESCE(:start_ts, 'epoch')
                      AND COALESCE(end_ts, 'epoch') = COALESCE(:end_ts, 'epoch')
                    """
                ),
                {
                    "data_type": data_type,
                    "inst_id": inst_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                },
            ).scalar()
            if exists:
                logger.info(
                    "DataGap 已存在，跳过登记: %s %s %s~%s",
                    data_type, inst_id, start_ts, end_ts,
                )
                return False
            conn.execute(
                text(
                    """
                    INSERT INTO data_gaps
                        (data_type, inst_id, start_ts, end_ts, gap_type,
                         status, detected_at, error_message)
                    VALUES
                        (:data_type, :inst_id, :start_ts, :end_ts, :gap_type,
                         'OPEN', :now, :err)
                    """
                ),
                {
                    "data_type": data_type,
                    "inst_id": inst_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "gap_type": gap_type,
                    "now": now,
                    "err": error_message,
                },
            )
            return True

    def mark_recovered(self, data_type: str, inst_id: str, start_ts, end_ts,
                       recovery_rows: int) -> None:
        """将 OPEN 缺口标记为 RECOVERED"""
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE data_gaps
                    SET status = 'RECOVERED', recovered_at = :now,
                        recovery_rows = :rows
                    WHERE data_type = :data_type AND inst_id = :inst_id
                      AND status = 'OPEN'
                      AND COALESCE(start_ts, 'epoch') = COALESCE(:start_ts, 'epoch')
                      AND COALESCE(end_ts, 'epoch') = COALESCE(:end_ts, 'epoch')
                    """
                ),
                {
                    "data_type": data_type,
                    "inst_id": inst_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "now": now,
                    "rows": recovery_rows,
                },
            )

    def mark_unrecoverable(self, data_type: str, inst_id: str, start_ts, end_ts,
                           error_message: str = None) -> None:
        """将 OPEN 缺口标记为 UNRECOVERABLE"""
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE data_gaps
                    SET status = 'UNRECOVERABLE', recovered_at = :now,
                        error_message = :err
                    WHERE data_type = :data_type AND inst_id = :inst_id
                      AND status = 'OPEN'
                      AND COALESCE(start_ts, 'epoch') = COALESCE(:start_ts, 'epoch')
                      AND COALESCE(end_ts, 'epoch') = COALESCE(:end_ts, 'epoch')
                    """
                ),
                {
                    "data_type": data_type,
                    "inst_id": inst_id,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "now": now,
                    "err": error_message,
                },
            )


class BaseRecovery:
    """Recovery 基类：统一事件记录与缺口登记"""

    data_type = None

    def __init__(self, client: Optional[OKXClient] = None):
        self.client = client or OKXClient()
        self.events = RecoveryEventStore()
        self.gaps = DataGapStore()


class TradeRecovery(BaseRecovery):
    """Trades 断线恢复（ID-based）"""

    data_type = "trades"

    def __init__(self, client: Optional[OKXClient] = None):
        super().__init__(client)

    def recover(self, inst_id: str, latest_trade_id: Optional[str] = None,
                reason: str = "WS_DISCONNECT") -> int:
        """执行恢复

        Args:
            inst_id: 产品ID
            latest_trade_id: 断线前最新的 trade_id
            reason: 恢复原因

        Returns:
            int: 回补的 trade 数量
        """
        if latest_trade_id is None:
            latest_trade_id = self._get_latest_trade_id(inst_id)
        if latest_trade_id is None:
            logger.info("No latest trade_id, skip recovery for %s", inst_id)
            return 0

        now = datetime.now(timezone.utc)
        recovery_id = self.events.start(
            "trades", inst_id, reason=reason,
            from_id=latest_trade_id, to_ts=now,
        )

        logger.info("Starting trade recovery: %s from trade_id=%s", inst_id, latest_trade_id)
        downloader = TradesDownloader(client=self.client)
        count = 0
        status = RECOVERY_STATUS_RECOVERED
        error = None
        try:
            count = downloader.download_range(
                inst_id=inst_id,
                start=now - timedelta(days=7),
                max_pages=20,
                after_trade_id=latest_trade_id,
            )
        except Exception as e:
            status = RECOVERY_STATUS_UNRECOVERABLE
            error = str(e)
            logger.error("Trade recovery failed for %s: %s", inst_id, e)

        self.events.finish(
            recovery_id, status, count, error_message=error,
            to_id=latest_trade_id,
        )
        logger.info("Trade recovery completed: %s | %d rows | %s", inst_id, count, status)
        return count

    def _get_latest_trade_id(self, inst_id: str) -> Optional[str]:
        with self.events.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT trade_id FROM trades
                    WHERE inst_id = :inst_id
                    ORDER BY ts DESC, trade_id DESC
                    LIMIT 1
                    """
                ),
                {"inst_id": inst_id},
            ).mappings().first()
            return row["trade_id"] if row else None


class TimeRangeRecovery(BaseRecovery):
    """时间范围恢复（OI / Mark / Index / Funding / Kline 用 REST 回补）"""

    def __init__(self, client: Optional[OKXClient] = None):
        super().__init__(client)

    def recover(self, data_type: str, inst_id: str,
                start: datetime, end: datetime,
                bar: str = "1D",
                reason: str = "WS_DISCONNECT") -> int:
        """执行时间范围恢复

        Returns:
            int: 回补的行数（0 表示无需回补或已由其他机制处理）
        """
        recovery_id = self.events.start(
            data_type, inst_id, reason=reason,
            from_ts=start, to_ts=end,
        )
        count = 0
        status = RECOVERY_STATUS_RECOVERED
        error = None
        try:
            if data_type == "oi":
                downloader = OpenInterestDownloader(client=self.client)
                count = downloader.download(inst_id=inst_id, bar=bar)
            elif data_type == "mark":
                downloader = MarkPriceDownloader(client=self.client)
                count = downloader.download_range(
                    inst_id=inst_id, bar=bar, start=start, end=end
                )
            elif data_type == "index":
                downloader = IndexPriceDownloader(client=self.client)
                count = downloader.download_range(
                    inst_id=inst_id, bar=bar, start=start, end=end
                )
            elif data_type == "funding":
                downloader = FundingRateDownloader(client=self.client)
                count = downloader.download_range(
                    inst_id=inst_id, start=start, end=end
                )
            else:
                status = RECOVERY_STATUS_UNRECOVERABLE
                error = f"Unsupported recovery data_type: {data_type}"
        except Exception as e:
            status = RECOVERY_STATUS_UNRECOVERABLE
            error = str(e)
            logger.error("TimeRange recovery failed: %s %s | %s", data_type, inst_id, e)

        if count > 0 or status == RECOVERY_STATUS_RECOVERED:
            self.gaps.mark_recovered(data_type, inst_id, start, end, count)
        elif status == RECOVERY_STATUS_UNRECOVERABLE:
            self.gaps.mark_unrecoverable(
                data_type, inst_id, start, end, error_message=error
            )

        self.events.finish(recovery_id, status, count, error_message=error)
        logger.info(
            "TimeRange recovery: %s %s | %d rows | %s", data_type, inst_id, count, status
        )
        return count


class OrderBookRecovery(BaseRecovery):
    """OrderBook 恢复

    触发 OrderBookHandler 重新同步（等待 action=snapshot）。
    """

    data_type = "orderbook"

    def __init__(self):
        super().__init__()

    def trigger_resync(self, inst_id: str, reason: str = "SEQ_GAP") -> None:
        """登记 OrderBook seq 缺口并记录事件（实际 resync 由 handler 状态机完成）"""
        now = datetime.now(timezone.utc)
        recovery_id = self.events.start(
            "orderbook", inst_id, reason=reason,
            from_ts=now - timedelta(seconds=5), to_ts=now,
        )
        self.gaps.register_open(
            "orderbook", inst_id, now - timedelta(seconds=5), now,
            gap_type=reason,
        )
        self.events.finish(
            recovery_id, RECOVERY_STATUS_RECOVERED, 0,
            error_message="triggered resync; waiting for action=snapshot",
        )


class RecoveryManager:
    """统一 Recovery 调度入口"""

    def __init__(self, client: Optional[OKXClient] = None):
        self.client = client or OKXClient()
        self.trade = TradeRecovery(client=self.client)
        self.time_range = TimeRangeRecovery(client=self.client)
        self.orderbook = OrderBookRecovery()

    def recover(self, data_type: str, inst_id: str, **kwargs) -> int:
        """按 data_type 分发恢复

        Args:
            data_type: trades / oi / mark / index / funding / orderbook
        """
        if data_type == "trades":
            return self.trade.recover(inst_id, **kwargs)
        if data_type == "orderbook":
            self.orderbook.trigger_resync(inst_id, reason=kwargs.get("reason", "SEQ_GAP"))
            return 0
        if data_type in ("oi", "mark", "index", "funding"):
            start = kwargs.get("start")
            end = kwargs.get("end")
            if start is None:
                start = datetime.now(timezone.utc) - timedelta(minutes=5)
            if end is None:
                end = datetime.now(timezone.utc)
            return self.time_range.recover(
                data_type, inst_id, start, end,
                bar=kwargs.get("bar", "1D"),
                reason=kwargs.get("reason", "WS_DISCONNECT"),
            )
        raise ValueError(f"Unsupported recovery data_type: {data_type}")

    def recover_all(self, inst_ids: List[str], data_types: List[str] = None) -> None:
        """批量恢复（启动时使用）"""
        data_types = data_types or ["trades"]
        for inst_id in inst_ids:
            for dt in data_types:
                try:
                    self.recover(dt, inst_id)
                except Exception as e:
                    logger.error("Recovery failed: %s %s | %s", dt, inst_id, e)
