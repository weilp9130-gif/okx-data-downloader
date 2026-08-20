"""资产服务：资产引导 + 增量/全量刷新（dataset_definition 驱动）

- asset_guidance：instruments(SWAP live USDT) × dataset_definition(enabled) → data_asset
- refresh：incremental（默认）增量计数；full 全量重算；latest_ts 回退自动转 full
- 状态判定：freshness_lag vs expected_freshness_sec → HEALTHY/WARNING/STALE；NO_DATA 灰标
"""

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import bindparam, text

from ..config.config import Config
from ..db.database import session_scope
from ..utils.logger import get_logger
from ..utils.time_utils import bar_to_seconds

logger = get_logger(__name__)

# dataset → market 表映射（白名单，防注入）
DATASET_TABLE_MAP = {
    "KLINE": "candles",
    "TRADES": "trades",
    "FUNDING_RATE": "funding_rates",
    "MARK_PRICE": "mark_prices",
    "INDEX_PRICE": "index_prices",
    "OPEN_INTEREST": "open_interest",
    "ORDER_BOOK": "order_book_snapshots",
}

# 无周期数据集（bar=''）
NO_BAR_DATASETS = ("TRADES", "FUNDING_RATE", "OPEN_INTEREST", "ORDER_BOOK")

MARKET = "SWAP"
EXCHANGE = "OKX"


# ------------------------------------------------------------------
# 种子：dataset_definition
# ------------------------------------------------------------------
def seed_dataset_definitions() -> int:
    """幂等种子 dataset_definition（lifespan 调用）"""
    from ..db.models import DatasetDefinition

    cfg = Config()
    kline_bars = cfg.download.kline_bars
    rows = []
    now = datetime.now(timezone.utc)

    def add(dataset: str, bar: str, table: str, interval: Optional[int]) -> None:
        if interval:
            freshness = max(2 * interval, 600)
        else:
            freshness = 3600
        rows.append({
            "dataset": dataset,
            "bar": bar,
            "version": "v1",
            "table_name": table,
            "primary_time_column": "ts",
            "source": "OKX",
            "interval_seconds": interval,
            "expected_freshness_sec": freshness,
            "retention_days": 0,
            "enabled": True,
            "created_at": now,
            "updated_at": now,
        })

    for bar in kline_bars:
        add("KLINE", bar, "candles", bar_to_seconds(bar))
        add("MARK_PRICE", bar, "mark_prices", bar_to_seconds(bar))
        add("INDEX_PRICE", bar, "index_prices", bar_to_seconds(bar))
    for ds in NO_BAR_DATASETS:
        add(ds, "", DATASET_TABLE_MAP[ds], None)

    count = 0
    with session_scope() as s:
        for r in rows:
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(DatasetDefinition).values(r)
            stmt = stmt.on_conflict_do_update(
                constraint="dataset_definition_pkey",
                set_={
                    "table_name": stmt.excluded.table_name,
                    "primary_time_column": stmt.excluded.primary_time_column,
                    "interval_seconds": stmt.excluded.interval_seconds,
                    "expected_freshness_sec": stmt.excluded.expected_freshness_sec,
                    "enabled": stmt.excluded.enabled,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            result = s.execute(stmt)
            count += result.rowcount or 0
    logger.info("dataset_definition 种子完成: %d 条（upsert）", len(rows))
    return len(rows)


# ------------------------------------------------------------------
# 资产引导
# ------------------------------------------------------------------
def _live_swap_inst_ids() -> List[str]:
    from ..db.models import Instrument

    with session_scope() as s:
        rows = (
            s.query(Instrument.inst_id)
            .filter(
                Instrument.inst_type == "SWAP",
                Instrument.state == "live",
                Instrument.settle_ccy == "USDT",
            )
            .all()
        )
        return [r[0] for r in rows]


def _enabled_definitions() -> List[dict]:
    from ..db.models import DatasetDefinition

    with session_scope() as s:
        rows = (
            s.query(DatasetDefinition)
            .filter(DatasetDefinition.enabled == True)  # noqa: E712
            .all()
        )
        return [
            {
                "dataset": r.dataset,
                "bar": r.bar,
                "table_name": r.table_name,
                "primary_time_column": r.primary_time_column,
                "interval_seconds": r.interval_seconds,
                "expected_freshness_sec": r.expected_freshness_sec,
            }
            for r in rows
        ]


def asset_guidance(store=None) -> int:
    """instruments(SWAP live USDT) × dataset_definition(enabled) 生成 data_asset

    批量：单次查询已有键 + add_all 批量插入，避免逐行 N+1 查询。
    """
    from ..db.models import DataAsset

    inst_ids = _live_swap_inst_ids()
    defs = _enabled_definitions()
    if not defs:
        logger.warning("无启用的 dataset_definition，跳过资产引导")
        return 0
    now = datetime.now(timezone.utc)
    created = 0
    with session_scope() as s:
        existing = {
            (r.exchange, r.market, r.inst_id, r.dataset, r.bar)
            for r in s.query(
                DataAsset.exchange, DataAsset.market,
                DataAsset.inst_id, DataAsset.dataset, DataAsset.bar,
            ).all()
        }
        missing = []
        for inst_id in inst_ids:
            for d in defs:
                key = (EXCHANGE, MARKET, inst_id, d["dataset"], d["bar"])
                if key in existing:
                    continue
                missing.append(DataAsset(
                    exchange=EXCHANGE, market=MARKET, inst_id=inst_id,
                    dataset=d["dataset"], bar=d["bar"],
                    created_at=now, updated_at=now,
                ))
        if missing:
            s.add_all(missing)
            created = len(missing)
    if created:
        logger.info("资产引导：新增 %d 条 data_asset", created)
    else:
        logger.info("资产引导：无新增")
    return created


# ------------------------------------------------------------------
# 状态判定（纯函数，供测试）
# ------------------------------------------------------------------
def determine_status(
    row_count: int,
    freshness_lag_sec: Optional[float],
    expected_freshness_sec: Optional[int],
    quality_score: Optional[float],
) -> str:
    """动态状态：NO_DATA / HEALTHY / WARNING / STALE / ERROR"""
    if row_count == 0:
        return "NO_DATA"
    if expected_freshness_sec and freshness_lag_sec is not None:
        ratio = freshness_lag_sec / float(expected_freshness_sec)
        if ratio > 5:
            return "STALE"
        if ratio > 1:
            return "WARNING"
    if quality_score is not None and quality_score < 90:
        return "WARNING"
    return "HEALTHY"


def _expected_freshness(defn: dict) -> int:
    if defn.get("expected_freshness_sec"):
        return int(defn["expected_freshness_sec"])
    interval = defn.get("interval_seconds")
    if interval:
        return max(2 * int(interval), 600)
    return 3600


# ------------------------------------------------------------------
# 刷新（增量为主，全量兜底）
# ------------------------------------------------------------------
def refresh_asset(inst_id: str, dataset: str, bar: str, mode: str = "incremental",
                  session=None) -> dict:
    """刷新单个资产，返回新的 state 摘要

    Args:
        session: 可复用 session（批量刷新时传入，避免每资产独立事务）
    """
    from ..db.models import DataAsset, DataAssetState

    defn = _get_definition(dataset, bar, session=session)
    if defn is None:
        raise ValueError(f"无 dataset_definition: {dataset}/{bar}")
    table = defn["table_name"]
    time_col = defn["primary_time_column"]
    if table not in set(DATASET_TABLE_MAP.values()):
        raise ValueError(f"非法 table_name: {table}")

    expected_freshness = _expected_freshness(defn)
    now = datetime.now(timezone.utc)

    if session is not None:
        return _refresh_with_session(
            session, inst_id, dataset, bar, table, time_col,
            expected_freshness, now, mode,
        )
    with session_scope() as s:
        return _refresh_with_session(
            s, inst_id, dataset, bar, table, time_col,
            expected_freshness, now, mode,
        )


def refresh_assets_batch(mode: str = "incremental", inst_id: str = None,
                         on_progress=None) -> dict:
    """批量刷新资产（单个事务）

    首次建资产 / full 模式：每张 market 表一次 GROUP BY 汇总（10 张表 = 10 次查询），
    避免逐资产 N+1；存量资产走增量 delta 计数。返回 {processed, failed}。
    """
    from ..db.models import DataAsset, DataAssetState

    defs_by_key = {
        (d["dataset"], d["bar"]): d for d in _enabled_definitions()
    }
    now = datetime.now(timezone.utc)
    processed = 0
    failed = 0
    with session_scope() as s:
        q = (
            s.query(DataAsset, DataAssetState)
            .outerjoin(DataAssetState, DataAssetState.asset_id == DataAsset.id)
        )
        if inst_id:
            q = q.filter(DataAsset.inst_id == inst_id)
        pairs = q.all()
        total = len(pairs)

        # 按表分组
        by_table = {}
        for asset, state in pairs:
            defn = defs_by_key.get((asset.dataset, asset.bar))
            if defn is None:
                continue
            if defn["table_name"] not in set(DATASET_TABLE_MAP.values()):
                continue
            by_table.setdefault(defn["table_name"], []).append((asset, state, defn))

        for table, items in by_table.items():
            time_col = items[0][2]["primary_time_column"]
            has_bar = table in ("candles", "mark_prices", "index_prices", "open_interest")
            # 首次建资产（state 为空/row_count=0）或 full → 表级 GROUP BY 汇总
            full_candidates = [
                (a, st, d) for a, st, d in items
                if mode == "full" or st is None or (st.row_count or 0) == 0
            ]
            group_stats = {}
            if full_candidates:
                inst_ids = sorted({a.inst_id for a, _st, _d in full_candidates})
                group_stats = _group_stats(
                    s, table, time_col, inst_ids, has_bar
                )

            for asset, state, defn in items:
                try:
                    key = (asset.inst_id, asset.bar) if has_bar else (asset.inst_id, "")
                    full = group_stats.get(key)
                    if mode == "full" or state is None or (state.row_count or 0) == 0:
                        full = full or {
                            "row_count": 0, "earliest_ts": None, "latest_ts": None,
                        }
                        _apply_full(s, asset, state, defn, full, now)
                    else:
                        _apply_incremental(s, asset, state, defn, now)
                    processed += 1
                except Exception as e:
                    failed += 1
                    logger.error("资产刷新失败: %s/%s/%s | %s",
                                 asset.inst_id, asset.dataset, asset.bar, e)
                if on_progress is not None:
                    on_progress(processed + failed, total)
    return {"processed": processed, "failed": failed}


def _group_stats(s, table: str, time_col: str, inst_ids: List[str],
                 has_bar: bool) -> dict:
    """表级 GROUP BY 汇总（限定 inst 范围，走 inst_id 索引）

    Returns:
        {(inst_id[, bar]): {row_count, earliest_ts, latest_ts}}
    """
    if not inst_ids:
        return {}
    if has_bar:
        rows = s.execute(text(
            f"SELECT inst_id, bar, COUNT(*) AS c, MIN({time_col}) AS mn, "
            f"MAX({time_col}) AS mx FROM {table} "
            f"WHERE inst_id IN :ids GROUP BY inst_id, bar"
        ).bindparams(bindparam("ids", expanding=True)), {"ids": inst_ids}).mappings().all()
        return {
            (r["inst_id"], r["bar"]): {
                "row_count": int(r["c"] or 0),
                "earliest_ts": r["mn"],
                "latest_ts": r["mx"],
            }
            for r in rows
        }
    rows = s.execute(text(
        f"SELECT inst_id, COUNT(*) AS c, MIN({time_col}) AS mn, "
        f"MAX({time_col}) AS mx FROM {table} "
        f"WHERE inst_id IN :ids GROUP BY inst_id"
    ).bindparams(bindparam("ids", expanding=True)), {"ids": inst_ids}).mappings().all()
    return {
        (r["inst_id"], ""): {
            "row_count": int(r["c"] or 0),
            "earliest_ts": r["mn"],
            "latest_ts": r["mx"],
        }
        for r in rows
    }


def _apply_full(s, asset, state, defn, full: dict, now) -> None:
    """按全量统计应用 state（full 模式或首次建资产）"""
    from ..db.models import DataAssetState

    if state is None:
        state = DataAssetState(asset_id=asset.id)
        s.add(state)
    state.row_count = full["row_count"]
    state.earliest_ts = full["earliest_ts"]
    state.latest_ts = full["latest_ts"]
    state.full_recount_at = now
    state.checked_at = now
    _finalize_state(state, defn, now)


def _apply_incremental(s, asset, state, defn, now) -> None:
    """增量：latest + delta 一次查询；latest 回退自动转 full"""
    from ..db.models import DataAssetState

    table = defn["table_name"]
    time_col = defn["primary_time_column"]
    has_bar = table in ("candles", "mark_prices", "index_prices", "open_interest")
    latest, delta = _latest_and_delta(
        s, table, time_col, asset.inst_id, asset.bar, state.checked_at
    )
    if (state.latest_ts is not None and latest is not None
            and latest < state.latest_ts):
        logger.info("latest_ts 回退(%s→%s)，转 full 重算: %s/%s",
                    state.latest_ts, latest, asset.inst_id, asset.dataset)
        full = _full_stats(s, table, time_col, asset.inst_id, asset.bar)
        _apply_full(s, asset, state, defn, full, now)
        return
    if delta:
        state.row_count = (state.row_count or 0) + delta
    if latest is not None:
        state.latest_ts = latest
    state.checked_at = now
    _finalize_state(state, defn, now)


def _finalize_state(state, defn, now) -> None:
    """状态判定（状态由 freshness_lag / quality_score 动态计算）"""
    lag = None
    if state.latest_ts is not None:
        lag = (now - state.latest_ts).total_seconds()
    score = float(state.quality_score) if state.quality_score is not None else None
    state.freshness_lag_sec = lag
    state.status = determine_status(
        int(state.row_count or 0), lag, _expected_freshness(defn), score
    )
    state.last_check_at = now


def _refresh_with_session(s, inst_id: str, dataset: str, bar: str,
                          table: str, time_col: str,
                          expected_freshness: int, now, mode: str) -> dict:
    """在给定 session 内刷新单个资产（不提交，由调用方统一 commit）"""
    from ..db.models import DataAsset, DataAssetState

    asset = (
        s.query(DataAsset)
        .filter(
            DataAsset.exchange == EXCHANGE, DataAsset.market == MARKET,
            DataAsset.inst_id == inst_id, DataAsset.dataset == dataset,
            DataAsset.bar == bar,
        )
        .first()
    )
    if asset is None:
        asset = DataAsset(
            exchange=EXCHANGE, market=MARKET, inst_id=inst_id,
            dataset=dataset, bar=bar, created_at=now, updated_at=now,
        )
        s.add(asset)
        s.flush()
    state = (
        s.query(DataAssetState)
        .filter(DataAssetState.asset_id == asset.id)
        .first()
    )
    if state is None:
        state = DataAssetState(asset_id=asset.id)
        s.add(state)

    prev_checked = state.checked_at

    if mode == "full":
        full = _full_stats(s, table, time_col, inst_id, bar)
        state.row_count = full["row_count"]
        state.earliest_ts = full["earliest_ts"]
        state.latest_ts = full["latest_ts"]
        state.full_recount_at = now
        state.checked_at = now
    else:
        # incremental：latest + delta 一次查询
        latest, delta = _latest_and_delta(
            s, table, time_col, inst_id, bar, prev_checked
        )
        if (state.latest_ts is not None and latest is not None
                and latest < state.latest_ts):
            # 数据被删 / latest 回退 → 自动转 full 重算
            logger.info("latest_ts 回退(%s→%s)，转 full 重算: %s/%s",
                        state.latest_ts, latest, inst_id, dataset)
            full = _full_stats(s, table, time_col, inst_id, bar)
            state.row_count = full["row_count"]
            state.earliest_ts = full["earliest_ts"]
            state.latest_ts = full["latest_ts"]
            state.full_recount_at = now
            state.checked_at = now
        elif (state.row_count or 0) == 0 and latest is not None:
            # 首次建资产 / 从未计数 → full
            full = _full_stats(s, table, time_col, inst_id, bar)
            state.row_count = full["row_count"]
            state.earliest_ts = full["earliest_ts"]
            state.latest_ts = full["latest_ts"]
            state.full_recount_at = now
            state.checked_at = now
        else:
            if delta:
                state.row_count = (state.row_count or 0) + delta
            if latest is not None:
                state.latest_ts = latest
            state.checked_at = now

    # 状态判定
    lag = None
    if state.latest_ts is not None:
        lag = (now - state.latest_ts).total_seconds()
    score = float(state.quality_score) if state.quality_score is not None else None
    state.freshness_lag_sec = lag
    state.status = determine_status(
        int(state.row_count or 0), lag, expected_freshness, score
    )
    state.last_check_at = now
    return {
        "inst_id": inst_id, "dataset": dataset, "bar": bar,
        "row_count": int(state.row_count or 0),
        "earliest_ts": state.earliest_ts, "latest_ts": state.latest_ts,
        "status": state.status, "freshness_lag_sec": lag,
    }


def _get_definition(dataset: str, bar: str, session=None) -> Optional[dict]:
    from ..db.models import DatasetDefinition

    def _query(s):
        r = (
            s.query(DatasetDefinition)
            .filter(
                DatasetDefinition.dataset == dataset,
                DatasetDefinition.bar == bar,
            )
            .first()
        )
        if r is None:
            return None
        return {
            "dataset": r.dataset,
            "bar": r.bar,
            "table_name": r.table_name,
            "primary_time_column": r.primary_time_column,
            "interval_seconds": r.interval_seconds,
            "expected_freshness_sec": r.expected_freshness_sec,
        }

    if session is not None:
        return _query(session)
    with session_scope() as s:
        return _query(s)


def _full_stats(s, table: str, time_col: str, inst_id: str, bar: str) -> dict:
    """全量 COUNT + MIN/MAX ts（bar 过滤按表特性：candles/mark/index 有 bar 列）"""
    has_bar = table in ("candles", "mark_prices", "index_prices", "open_interest")
    if has_bar:
        row = s.execute(
            text(
                f"SELECT COUNT(*) AS c, MIN({time_col}) AS mn, MAX({time_col}) AS mx "
                f"FROM {table} WHERE inst_id = :i AND bar = :b"
            ),
            {"i": inst_id, "b": bar},
        ).mappings().one()
    else:
        row = s.execute(
            text(
                f"SELECT COUNT(*) AS c, MIN({time_col}) AS mn, MAX({time_col}) AS mx "
                f"FROM {table} WHERE inst_id = :i"
            ),
            {"i": inst_id},
        ).mappings().one()
    return {
        "row_count": int(row["c"] or 0),
        "earliest_ts": row["mn"],
        "latest_ts": row["mx"],
    }


def _latest_and_delta(s, table: str, time_col: str, inst_id: str, bar: str,
                      since: Optional[datetime]) -> tuple:
    """一次查询返回 (MAX(time_col), COUNT WHERE time_col > since)"""
    has_bar = table in ("candles", "mark_prices", "index_prices", "open_interest")
    if has_bar:
        row = s.execute(
            text(
                f"SELECT MAX({time_col}) AS mx, "
                f"COUNT(*) FILTER (WHERE {time_col} > :since) AS delta "
                f"FROM {table} WHERE inst_id = :i AND bar = :b"
            ),
            {"i": inst_id, "b": bar, "since": since},
        ).mappings().one()
    else:
        row = s.execute(
            text(
                f"SELECT MAX({time_col}) AS mx, "
                f"COUNT(*) FILTER (WHERE {time_col} > :since) AS delta "
                f"FROM {table} WHERE inst_id = :i"
            ),
            {"i": inst_id, "since": since},
        ).mappings().one()
    return row["mx"], int(row["delta"] or 0)
