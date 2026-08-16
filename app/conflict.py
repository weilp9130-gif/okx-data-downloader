"""Raw 数据冲突检测（DATA_CONFLICT）

Raw 数据是最终事实来源，禁止静默覆盖：
    相同 business key
        ├── raw_hash 相同 → duplicate（ON CONFLICT DO NOTHING）
        └── raw_hash 不同 → DATA_CONFLICT（登记冲突，保留原值）
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from .database import get_engine
from .utils.logger import get_logger

logger = get_logger(__name__)

CONFLICT_STATUS_OPEN = "OPEN"
CONFLICT_STATUS_RESOLVED = "RESOLVED"


def canonical_hash(payload: dict) -> str:
    """计算 canonical raw_json 的 SHA256"""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


class DataConflictDetector:
    """Raw 表冲突检测器

    以 trades 为首个接入表：业务键 (inst_id, trade_id, ts)。
    """

    def __init__(self):
        self.engine = get_engine()

    def detect_trades(self, rows: List[dict]) -> Tuple[List[dict], List[dict]]:
        """检查待写入 trades 是否与库内同键记录 payload 冲突

        Args:
            rows: 归一化后的 trade 行（含 raw_hash / raw_json）

        Returns:
            (safe_rows, conflicts): safe_rows 可安全写入；conflicts 为冲突详情
        """
        if not rows:
            return [], []

        keys = [(r["inst_id"], r["trade_id"], r["ts"]) for r in rows]
        existing = self._fetch_existing_trades(keys)
        if not existing:
            return rows, []

        safe_rows: List[dict] = []
        conflicts: List[dict] = []
        for row in rows:
            key = (row["inst_id"], row["trade_id"], row["ts"])
            prev = existing.get(key)
            if prev is None:
                safe_rows.append(row)
                continue
            prev_hash = prev.get("raw_hash")
            new_hash = row.get("raw_hash")
            if prev_hash and new_hash and prev_hash != new_hash:
                conflicts.append({
                    "table_name": "trades",
                    "inst_id": row["inst_id"],
                    "biz_key": f"{row['trade_id']}@{row['ts'].isoformat()}",
                    "existing_hash": prev_hash,
                    "incoming_hash": new_hash,
                    "existing_payload": prev.get("raw_json"),
                    "incoming_payload": row.get("raw_json"),
                    "source": row.get("source"),
                })
            else:
                # hash 相同视为 duplicate，交由 ON CONFLICT 处理
                safe_rows.append(row)

        if conflicts:
            self.register(conflicts)
        return safe_rows, conflicts

    def _fetch_existing_trades(self, keys: List[Tuple]) -> Dict[Tuple, dict]:
        """批量查询库内已存在的 trades（按业务键）"""
        if not keys:
            return {}
        inst_ids = sorted({k[0] for k in keys})
        trade_ids = sorted({k[1] for k in keys})
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT inst_id, trade_id, ts, raw_hash, raw_json
                    FROM trades
                    WHERE inst_id = ANY(:inst_ids)
                      AND trade_id = ANY(:trade_ids)
                    """
                ),
                {"inst_ids": inst_ids, "trade_ids": trade_ids},
            ).mappings().all()
        return {(r["inst_id"], r["trade_id"], r["ts"]): dict(r) for r in rows}

    def register(self, conflicts: List[dict]) -> int:
        """登记冲突（同键 OPEN 冲突已存在则跳过）"""
        if not conflicts:
            return 0
        now = datetime.now(timezone.utc)
        inserted = 0
        with self.engine.begin() as conn:
            for c in conflicts:
                exists = conn.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM data_conflicts
                        WHERE table_name = :table_name AND inst_id = :inst_id
                          AND biz_key = :biz_key AND status = 'OPEN'
                        """
                    ),
                    {
                        "table_name": c["table_name"],
                        "inst_id": c["inst_id"],
                        "biz_key": c["biz_key"],
                    },
                ).scalar()
                if exists:
                    continue
                conn.execute(
                    text(
                        """
                        INSERT INTO data_conflicts
                            (table_name, inst_id, biz_key, existing_hash, incoming_hash,
                             existing_payload, incoming_payload, source, detected_at, status)
                        VALUES
                            (:table_name, :inst_id, :biz_key, :existing_hash, :incoming_hash,
                             CAST(:existing_payload AS JSONB), CAST(:incoming_payload AS JSONB),
                             :source, :now, 'OPEN')
                        """
                    ),
                    {
                        "table_name": c["table_name"],
                        "inst_id": c["inst_id"],
                        "biz_key": c["biz_key"],
                        "existing_hash": c.get("existing_hash"),
                        "incoming_hash": c.get("incoming_hash"),
                        "existing_payload": json.dumps(
                            c.get("existing_payload"), ensure_ascii=False
                        ),
                        "incoming_payload": json.dumps(
                            c.get("incoming_payload"), ensure_ascii=False
                        ),
                        "source": c.get("source"),
                        "now": now,
                    },
                )
                inserted += 1
                logger.error(
                    "DATA_CONFLICT: table=%s inst=%s key=%s existing_hash=%s incoming_hash=%s",
                    c["table_name"], c["inst_id"], c["biz_key"],
                    (c.get("existing_hash") or "")[:12],
                    (c.get("incoming_hash") or "")[:12],
                )
        return inserted

    def open_count(self, table_name: Optional[str] = None,
                   inst_id: Optional[str] = None) -> int:
        """统计未处理冲突数量"""
        sql = "SELECT COUNT(*) FROM data_conflicts WHERE status = 'OPEN'"
        params: Dict[str, str] = {}
        if table_name:
            sql += " AND table_name = :table_name"
            params["table_name"] = table_name
        if inst_id:
            sql += " AND inst_id = :inst_id"
            params["inst_id"] = inst_id
        with self.engine.connect() as conn:
            return int(conn.execute(text(sql), params).scalar() or 0)
