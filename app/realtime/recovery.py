"""WebSocket 断线恢复模块"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from ..database import get_engine
from ..downloader.trades import TradesDownloader
from ..okx_client import OKXClient
from ..utils.logger import get_logger

logger = get_logger(__name__)


class TradeRecovery:
    """Trades 断线恢复

    基于最新 trade_id 用 REST history-trades 回补缺口。
    """

    def __init__(self, client: Optional[OKXClient] = None):
        self.client = client or OKXClient()
        self.engine = get_engine()

    def recover(self, inst_id: str, latest_trade_id: Optional[str] = None) -> int:
        """执行恢复

        Args:
            inst_id: 产品ID
            latest_trade_id: 断线前最新的 trade_id

        Returns:
            int: 回补的 trade 数量
        """
        if latest_trade_id is None:
            latest_trade_id = self._get_latest_trade_id(inst_id)
        if latest_trade_id is None:
            logger.info("No latest trade_id, skip recovery for %s", inst_id)
            return 0

        logger.info("Starting trade recovery: %s from trade_id=%s", inst_id, latest_trade_id)
        downloader = TradesDownloader(client=self.client)
        # 从最新 trade_id 之后开始恢复，限制页数避免回溯过深
        count = downloader.download_range(
            inst_id=inst_id,
            start=datetime.now(timezone.utc).replace(year=2026, month=1, day=1),
            max_pages=20,
            after_trade_id=latest_trade_id,
        )
        logger.info("Trade recovery completed: %s recovered %d rows", inst_id, count)
        return count

    def _get_latest_trade_id(self, inst_id: str) -> Optional[str]:
        with self.engine.connect() as conn:
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
