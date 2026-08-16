"""数据质量验证器

实现 Phase 3 数据质量三层验证：
- Level 1：结构完整性（NULL、duplicate、invalid value）
- Level 2：时序完整性（timestamp regression、unexpected gap、trade ID anomaly）
- Level 3：跨源一致性（trade-derived volume vs candle volume）
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..database import get_engine
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityValidator:
    """数据质量验证器"""

    def __init__(self):
        self.engine = get_engine()
        self.report: Dict[str, Any] = {}

    def validate(self, data_type: str, inst_id: str, bar: Optional[str] = None) -> Dict[str, Any]:
        """对指定数据类型执行完整质量验证

        Args:
            data_type: instruments / candles / funding / mark / index / oi / trades
            inst_id: 产品ID
            bar: 时间粒度（如适用）

        Returns:
            dict: 验证报告
        """
        self.report = {
            "data_type": data_type,
            "inst_id": inst_id,
            "bar": bar,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "level1": {},
            "level2": {},
            "level3": {},
        }

        if data_type == "instruments":
            self._validate_instruments(inst_id)
        elif data_type == "candles":
            self._validate_candles(inst_id, bar or "1m")
        elif data_type == "funding":
            self._validate_funding(inst_id)
        elif data_type == "mark":
            self._validate_mark_prices(inst_id, bar or "1D")
        elif data_type == "index":
            self._validate_index_prices(inst_id, bar or "1D")
        elif data_type == "oi":
            self._validate_open_interest(inst_id)
        elif data_type == "trades":
            self._validate_trades(inst_id)
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")

        self._save_state(data_type, inst_id)
        return self.report

    # ------------------------------------------------------------------
    # Level 1: 结构完整性
    # ------------------------------------------------------------------
    def _level1(self, table: str, inst_id: str, bar: Optional[str] = None) -> Dict[str, Any]:
        """通用 Level 1 结构完整性检查"""
        params = {"inst_id": inst_id}
        bar_filter = "AND bar = :bar" if bar else ""
        if bar:
            params["bar"] = bar

        with self.engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE inst_id = :inst_id {bar_filter}"),
                params,
            ).scalar()

            duplicate = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT inst_id{', bar' if bar else ''}, ts, COUNT(*) c
                        FROM {table}
                        WHERE inst_id = :inst_id {bar_filter}
                        GROUP BY inst_id{', bar' if bar else ''}, ts
                        HAVING COUNT(*) > 1
                    ) t
                    """
                ),
                params,
            ).scalar()

        result = {
            "total": total,
            "duplicate": duplicate or 0,
        }
        self.report["level1"][table] = result
        return result

    def _validate_instruments(self, inst_id: str) -> None:
        """Instruments 质量检查"""
        with self.engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM instruments")).scalar()
            unique = conn.execute(text("SELECT COUNT(DISTINCT inst_id) FROM instruments")).scalar()
            nulls = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM instruments
                    WHERE inst_id IS NULL OR inst_type IS NULL OR state IS NULL
                    """
                )
            ).scalar()

        self.report["level1"]["instruments"] = {
            "total": total,
            "unique": unique,
            "duplicate": total - unique,
            "null_fields": nulls,
        }

    def _validate_candles(self, inst_id: str, bar: str) -> None:
        """Candles 质量检查"""
        self._level1("candles", inst_id, bar)
        with self.engine.connect() as conn:
            nulls = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM candles
                    WHERE inst_id = :inst_id AND bar = :bar
                    AND (o IS NULL OR h IS NULL OR l IS NULL OR c IS NULL OR vol IS NULL)
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

            invalid = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM candles
                    WHERE inst_id = :inst_id AND bar = :bar
                    AND (o <= 0 OR h <= 0 OR l <= 0 OR c <= 0 OR vol < 0)
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

            regression = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT ts, LAG(ts) OVER (ORDER BY ts) prev_ts
                        FROM candles
                        WHERE inst_id = :inst_id AND bar = :bar
                    ) t
                    WHERE ts < prev_ts
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

        self.report["level2"]["candles"] = {
            "nulls": nulls,
            "invalid_price": invalid,
            "timestamp_regression": regression,
        }

    def _validate_funding(self, inst_id: str) -> None:
        """Funding rates 质量检查"""
        self._level1("funding_rates", inst_id)
        with self.engine.connect() as conn:
            min_max = conn.execute(
                text(
                    """
                    SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts
                    FROM funding_rates
                    WHERE inst_id = :inst_id
                    """
                ),
                {"inst_id": inst_id},
            ).mappings().one()

        self.report["level2"]["funding_rates"] = {
            "min_ts": min_max["min_ts"].isoformat() if min_max["min_ts"] else None,
            "max_ts": min_max["max_ts"].isoformat() if min_max["max_ts"] else None,
        }

    def _validate_mark_prices(self, inst_id: str, bar: str) -> None:
        """Mark prices 质量检查"""
        self._level1("mark_prices", inst_id, bar)
        self.report["level2"]["mark_prices"] = self._price_temporal_checks("mark_prices", inst_id, bar)

    def _validate_index_prices(self, inst_id: str, bar: str) -> None:
        """Index prices 质量检查"""
        self._level1("index_prices", inst_id, bar)
        self.report["level2"]["index_prices"] = self._price_temporal_checks("index_prices", inst_id, bar)

    def _price_temporal_checks(self, table: str, inst_id: str, bar: str) -> Dict[str, Any]:
        """K线类表时序检查"""
        with self.engine.connect() as conn:
            nulls = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE inst_id = :inst_id AND bar = :bar
                    AND (o IS NULL OR h IS NULL OR l IS NULL OR c IS NULL)
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

            invalid = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE inst_id = :inst_id AND bar = :bar
                    AND (o <= 0 OR h <= 0 OR l <= 0 OR c <= 0)
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

            regression = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT ts, LAG(ts) OVER (ORDER BY ts) prev_ts
                        FROM {table}
                        WHERE inst_id = :inst_id AND bar = :bar
                    ) t
                    WHERE ts < prev_ts
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).scalar()

            min_max = conn.execute(
                text(
                    f"""
                    SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts
                    FROM {table}
                    WHERE inst_id = :inst_id AND bar = :bar
                    """
                ),
                {"inst_id": inst_id, "bar": bar},
            ).mappings().one()

        return {
            "nulls": nulls,
            "invalid_price": invalid,
            "timestamp_regression": regression,
            "min_ts": min_max["min_ts"].isoformat() if min_max["min_ts"] else None,
            "max_ts": min_max["max_ts"].isoformat() if min_max["max_ts"] else None,
        }

    def _validate_open_interest(self, inst_id: str) -> None:
        """Open Interest 质量检查"""
        # open_interest 业务唯一键为 (inst_id, bar, ts)，需按 bar 分组
        with self.engine.connect() as conn:
            total = conn.execute(
                text(
                    "SELECT COUNT(*) FROM open_interest WHERE inst_id = :inst_id"
                ),
                {"inst_id": inst_id},
            ).scalar()

            duplicate = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT inst_id, bar, ts, COUNT(*) c
                        FROM open_interest
                        WHERE inst_id = :inst_id
                        GROUP BY inst_id, bar, ts
                        HAVING COUNT(*) > 1
                    ) t
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

        self.report["level1"]["open_interest"] = {
            "total": total,
            "duplicate": duplicate or 0,
        }

        with self.engine.connect() as conn:
            invalid = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM open_interest
                    WHERE inst_id = :inst_id AND (oi IS NULL OR oi < 0)
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

        self.report["level2"]["open_interest"] = {"invalid_oi": invalid}

    def _validate_trades(self, inst_id: str) -> None:
        """Trades 质量检查"""
        # trades 业务唯一键为 (inst_id, trade_id, ts)，不能简单按 ts 分组
        with self.engine.connect() as conn:
            total = conn.execute(
                text("SELECT COUNT(*) FROM trades WHERE inst_id = :inst_id"),
                {"inst_id": inst_id},
            ).scalar()

            duplicate = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT inst_id, trade_id, ts, COUNT(*) c
                        FROM trades
                        WHERE inst_id = :inst_id
                        GROUP BY inst_id, trade_id, ts
                        HAVING COUNT(*) > 1
                    ) t
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

        self.report["level1"]["trades"] = {
            "total": total,
            "duplicate": duplicate or 0,
        }

        with self.engine.connect() as conn:
            nulls = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM trades
                    WHERE inst_id = :inst_id
                    AND (px IS NULL OR sz IS NULL OR side IS NULL OR ts IS NULL)
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

            invalid = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM trades
                    WHERE inst_id = :inst_id AND (px <= 0 OR sz <= 0)
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

            dup_trade_id = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT trade_id, COUNT(*) c
                        FROM trades
                        WHERE inst_id = :inst_id
                        GROUP BY trade_id
                        HAVING COUNT(*) > 1
                    ) t
                    """
                ),
                {"inst_id": inst_id},
            ).scalar()

        self.report["level2"]["trades"] = {
            "nulls": nulls,
            "invalid_price_size": invalid,
            "duplicate_trade_id": dup_trade_id,
        }

    def cross_source_volume_check(
        self, inst_id: str, bar: str, start: datetime, end: datetime
    ) -> Dict[str, Any]:
        """Level 3：trades 派生成交量 vs candles 成交量对比

        Args:
            inst_id: 产品ID
            bar: 时间粒度
            start: 开始时间
            end: 结束时间

        Returns:
            dict: 包含差异比率的报告
        """
        from ..utils.time_utils import bar_to_seconds, utc_ms_timestamp

        interval = bar_to_seconds(bar)
        start_ms = utc_ms_timestamp(start)
        end_ms = utc_ms_timestamp(end)

        with self.engine.connect() as conn:
            # trades 派生成交量（合约张数，与 OKX sz 同单位）
            trade_vol = conn.execute(
                text(
                    """
                    SELECT
                        to_timestamp(
                            floor(EXTRACT(EPOCH FROM ts) / :interval)::bigint * :interval
                        ) AS bucket,
                        SUM(sz) AS vol
                    FROM trades
                    WHERE inst_id = :inst_id AND ts BETWEEN :start AND :end
                    GROUP BY bucket
                    ORDER BY bucket
                    """
                ),
                {
                    "inst_id": inst_id,
                    "interval": interval,
                    "start": start,
                    "end": end,
                },
            ).mappings().all()

            # candle 成交量
            candle_vol = conn.execute(
                text(
                    """
                    SELECT ts, vol
                    FROM candles
                    WHERE inst_id = :inst_id AND bar = :bar AND ts BETWEEN :start AND :end
                    ORDER BY ts
                    """
                ),
                {"inst_id": inst_id, "bar": bar, "start": start, "end": end},
            ).mappings().all()

        trade_map = {row["bucket"]: float(row["vol"] or 0) for row in trade_vol}
        candle_map = {row["ts"]: float(row["vol"] or 0) for row in candle_vol}

        diffs = []
        for ts, c_vol in candle_map.items():
            t_vol = trade_map.get(ts, 0)
            if c_vol == 0:
                ratio = 0.0 if t_vol == 0 else float("inf")
            else:
                ratio = abs(t_vol - c_vol) / c_vol
            diffs.append({"ts": ts.isoformat(), "candle_vol": c_vol, "trade_vol": t_vol, "ratio": ratio})

        self.report["level3"]["cross_source_volume"] = {
            "buckets_checked": len(diffs),
            "max_ratio": max((d["ratio"] for d in diffs), default=0.0),
            "samples": diffs[:5],
        }
        return self.report["level3"]["cross_source_volume"]

    def _save_state(self, data_type: str, inst_id: str) -> None:
        """保存验证状态到 data_quality_state"""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from ..models import DataQualityState

        now = datetime.now(timezone.utc)
        row = {
            "data_type": data_type,
            "inst_id": inst_id,
            "last_success_at": now,
            "error_count": 0,
            "status": "HEALTHY",
            "updated_at": now,
        }
        stmt = pg_insert(DataQualityState).values(row)
        stmt = stmt.on_conflict_do_update(
            index_elements=["data_type", "inst_id"],
            set_={
                "last_success_at": stmt.excluded.last_success_at,
                "error_count": 0,
                "status": "HEALTHY",
                "updated_at": stmt.excluded.updated_at,
            },
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    # ------------------------------------------------------------------
    # Phase 9: 回归检查与索引验证
    # ------------------------------------------------------------------
    def collect_issues(self, report: Dict[str, Any]) -> List[str]:
        """从报告中提取阻断性问题（用于回归测试 --fail-on-issue）

        判定为问题的指标：duplicate / nulls / invalid_* / timestamp_regression
        / duplicate_trade_id / null_fields，只要 > 0 即视为问题。
        """
        issue_keys = (
            "duplicate",
            "nulls",
            "null_fields",
            "invalid_price",
            "invalid_price_size",
            "invalid_oi",
            "timestamp_regression",
            "duplicate_trade_id",
        )
        issues: List[str] = []
        data_type = report.get("data_type")
        for level in ("level1", "level2", "level3"):
            for table, metrics in (report.get(level) or {}).items():
                if not isinstance(metrics, dict):
                    continue
                for key, value in metrics.items():
                    if key not in issue_keys:
                        continue
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):
                        continue
                    if numeric > 0:
                        issues.append(
                            f"{data_type}/{level}/{table}/{key}={value}"
                        )
        return issues

    # 小表 PostgreSQL 规划器会优先 Seq Scan，此阈值以下不判定为索引缺失
    INDEX_CHECK_MIN_ROWS = 1000

    # EXPLAIN ANALYZE 会真实执行查询，超时保护避免大表 Seq Scan 卡死
    INDEX_CHECK_TIMEOUT_MS = 15000

    def _estimate_rows(self, conn, table: str) -> int:
        """用 planner 统计估算行数（含 hypertable chunks），避免精确 COUNT(*) 全表扫描"""
        try:
            return int(conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(c.reltuples), 0)::bigint
                    FROM pg_class c
                    WHERE c.oid = CAST(:table AS regclass)
                       OR c.oid IN (
                            SELECT inhrelid FROM pg_inherits
                            WHERE inhparent = CAST(:table AS regclass)
                       )
                    """
                ),
                {"table": table},
            ).scalar() or 0)
        except Exception:
            return 0

    def explain_index_usage(self, inst_id: str, tables: Optional[List[str]] = None
                            ) -> Dict[str, Any]:
        """验证 (inst_id, ts) 范围查询是否走索引

        先用 EXPLAIN（不执行）判定扫描方式，再用带 statement_timeout 的
        EXPLAIN ANALYZE 获取真实执行计划；后者超时不影响索引判定。

        小表（估算行数 < INDEX_CHECK_MIN_ROWS）走 Seq Scan 属规划器正常选择，
        标记 skipped=True，不视为索引问题。

        Returns:
            dict: {table: {"uses_index": bool, "seq_scan": bool, "rows": int,
                           "skipped": bool, "plan": str, "analyzed": bool}}
        """
        tables = tables or [
            "candles",
            "funding_rates",
            "trades",
            "mark_prices",
            "index_prices",
            "open_interest",
            "open_interest_realtime",
            "trade_aggregates",
            "order_book_snapshots",
        ]
        query = """
            SELECT * FROM {table}
            WHERE inst_id = :inst_id
              AND ts BETWEEN now() - INTERVAL '7 days' AND now()
            ORDER BY ts DESC
            LIMIT 100
        """
        results: Dict[str, Any] = {}
        for table in tables:
            # 每个表用独立连接，避免超时/取消污染后续查询
            try:
                with self.engine.connect() as conn:
                    row_count = self._estimate_rows(conn, table)
                    plan_rows = conn.execute(
                        text("EXPLAIN " + query.format(table=table)),
                        {"inst_id": inst_id},
                    ).fetchall()
                    plan = "\n".join(r[0] for r in plan_rows)
            except Exception as e:
                results[table] = {"error": str(e)}
                continue

            analyzed_plan = None
            try:
                with self.engine.connect() as conn:
                    conn.execute(
                        text(f"SET statement_timeout = {self.INDEX_CHECK_TIMEOUT_MS}")
                    )
                    analyze_rows = conn.execute(
                        text("EXPLAIN ANALYZE " + query.format(table=table)),
                        {"inst_id": inst_id},
                    ).fetchall()
                    analyzed_plan = "\n".join(r[0] for r in analyze_rows)
            except Exception as e:
                logger.warning("EXPLAIN ANALYZE 超时/失败: %s | %s", table, e)

            effective_plan = analyzed_plan or plan
            uses_index = "Index Scan" in plan or "Index Only Scan" in plan
            results[table] = {
                "uses_index": uses_index,
                "seq_scan": "Seq Scan" in plan,
                "rows": row_count,
                "skipped": row_count < self.INDEX_CHECK_MIN_ROWS,
                "analyzed": analyzed_plan is not None,
                "plan": effective_plan,
            }
        self.report.setdefault("index_check", {}).update(results)
        return results
