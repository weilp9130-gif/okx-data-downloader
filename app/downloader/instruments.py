"""Instruments 下载模块

负责从 OKX 拉取产品/交易对信息，经归一化、校验后幂等写入
PostgreSQL 的 instruments 表。
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import Config
from ..database import get_engine
from ..models import Instrument
from ..okx_client import OKXClient
from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime

logger = get_logger(__name__)


class InstrumentDownloader:
    """Instruments 下载器"""

    def __init__(self, client: Optional[OKXClient] = None, cfg: Optional[Config] = None):
        self.client = client or OKXClient()
        self.cfg = cfg or Config()

    def download(self, inst_type: str = "SWAP") -> int:
        """下载指定类型（或多个类型）的 instruments 并入库

        Args:
            inst_type: 产品类型，如 SWAP / SPOT / FUTURES / OPTION / MARGIN

        Returns:
            int: 写入/更新的数量
        """
        raw = self.client.get_instruments(inst_type=inst_type)
        if not raw:
            logger.warning("OKX 返回空的 instruments 列表: inst_type=%s", inst_type)
            return 0

        normalized = [normalize_instrument(item) for item in raw]
        normalized = [n for n in normalized if n is not None]

        if not normalized:
            logger.warning("Instruments 归一化后无有效数据: inst_type=%s", inst_type)
            return 0

        written = self._upsert(normalized)
        logger.info(
            "Instruments 下载完成: inst_type=%s | 共 %d 条 | 写入 %d 条",
            inst_type, len(normalized), written
        )
        return written

    def _upsert(self, records: List[Dict[str, Any]]) -> int:
        """批量 UPSERT instruments 表（幂等）"""
        if not records:
            return 0

        now = datetime.now(timezone.utc)
        rows = []
        for r in records:
            r["updated_at"] = now
            rows.append(r)

        stmt = pg_insert(Instrument).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inst_id"],
            set_={
                "inst_type": stmt.excluded.inst_type,
                "base_ccy": stmt.excluded.base_ccy,
                "quote_ccy": stmt.excluded.quote_ccy,
                "settle_ccy": stmt.excluded.settle_ccy,
                "ct_val": stmt.excluded.ct_val,
                "ct_mult": stmt.excluded.ct_mult,
                "tick_sz": stmt.excluded.tick_sz,
                "lot_sz": stmt.excluded.lot_sz,
                "min_sz": stmt.excluded.min_sz,
                "state": stmt.excluded.state,
                "list_time": stmt.excluded.list_time,
                "exp_time": stmt.excluded.exp_time,
                "lever": stmt.excluded.lever,
                "max_lmt_sz": stmt.excluded.max_lmt_sz,
                "max_mkt_sz": stmt.excluded.max_mkt_sz,
                "raw_json": stmt.excluded.raw_json,
                "updated_at": stmt.excluded.updated_at,
            },
        )

        engine = get_engine()
        with engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount or 0


def _to_decimal(value):
    """将字符串/数字转为可写入 NUMERIC 的类型，失败返回 None"""
    if value is None or value == "":
        return None
    try:
        return value
    except Exception:
        return None


def _parse_timestamp(value):
    """解析 OKX 毫秒时间戳字符串为 UTC datetime"""
    if value is None or value == "":
        return None
    try:
        return ms_to_datetime(int(value))
    except (ValueError, TypeError):
        return None


def normalize_instrument(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """将 OKX instruments 接口原始 JSON 归一化为数据库模型字段

    Args:
        raw: OKX /api/v5/public/instruments 单条数据

    Returns:
        dict 或 None（字段缺失/非法时丢弃）
    """
    inst_id = raw.get("instId")
    inst_type = raw.get("instType")

    if not inst_id or not inst_type:
        logger.warning("跳过非法 instrument: inst_id=%s, inst_type=%s", inst_id, inst_type)
        return None

    return {
        "inst_id": inst_id,
        "inst_type": inst_type,
        "base_ccy": raw.get("baseCcy") or None,
        "quote_ccy": raw.get("quoteCcy") or None,
        "settle_ccy": raw.get("settleCcy") or None,
        "ct_val": _to_decimal(raw.get("ctVal")),
        "ct_mult": _to_decimal(raw.get("ctMult")),
        "tick_sz": _to_decimal(raw.get("tickSz")),
        "lot_sz": _to_decimal(raw.get("lotSz")),
        "min_sz": _to_decimal(raw.get("minSz")),
        "state": raw.get("state") or None,
        "list_time": _parse_timestamp(raw.get("listTime")),
        "exp_time": _parse_timestamp(raw.get("expTime")),
        "lever": _to_decimal(raw.get("lever")),
        "max_lmt_sz": _to_decimal(raw.get("maxLmtSz")),
        "max_mkt_sz": _to_decimal(raw.get("maxMktSz")),
        "raw_json": raw,
    }
