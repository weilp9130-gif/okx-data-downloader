"""质量评分：quality_report JSON → 四维评分（纯函数）+ 写回 data_asset_state

    completeness = 1 − missing/expected（expected=0 → 1）
    validity     = 1 − (dup+null+invalid)/total（total=0 → 1）
    consistency  = 1 − cross_source.max_ratio（未跑 → 1）
    freshness    = clamp01(1 − lag/expected_freshness_sec)
    score        = (0.4C + 0.3V + 0.2K + 0.1F) × 100，1 位小数
"""

import json
from datetime import datetime, timezone
from typing import Dict, Optional

from ..db.database import session_scope
from ..utils.logger import get_logger

logger = get_logger(__name__)

# quality_report data_type → 资产 dataset
REPORT_DATASET_MAP = {
    "candles": "KLINE",
    "funding": "FUNDING_RATE",
    "mark": "MARK_PRICE",
    "index": "INDEX_PRICE",
    "oi": "OPEN_INTEREST",
    "trades": "TRADES",
}


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _safe_definition(dataset: str, bar: str = "") -> Optional[dict]:
    """读取 dataset_definition（表不存在/DB 不可用 → None）"""
    try:
        from .assets import _get_definition

        return _get_definition(dataset, bar)
    except Exception:
        return None


def _interval_for(dataset: str) -> Optional[int]:
    defn = _safe_definition(dataset, "")
    if defn is None or not defn.get("interval_seconds"):
        return None
    return int(defn["interval_seconds"])


def score_data_type(report: dict, dataset: str,
                    cross_source_volume: Optional[dict] = None) -> dict:
    """计算单个 data_type 的四维评分与总分"""
    data_type = report.get("data_type", dataset)
    level1 = report.get("level1") or {}
    level2 = report.get("level2") or {}
    level3 = report.get("level3") or {}

    table = next(iter(level1), data_type)
    l1 = level1.get(table) or {}
    l2 = level2.get(table) or {}

    total = int(l1.get("total", 0) or 0)
    duplicate = int(l1.get("duplicate", 0) or 0)
    nulls = int(l2.get("nulls", 0) or 0)
    invalid = int(
        l2.get("invalid_price", 0) or l2.get("invalid_price_size", 0)
        or l2.get("invalid_oi", 0) or 0
    )
    regression = int(l2.get("timestamp_regression", 0) or 0)
    invalid_total = duplicate + nulls + invalid + regression

    # ---- completeness：基于 min/max ts 跨度与 interval 的期望行数 ----
    expected = total
    min_ts = l2.get("min_ts")
    max_ts = l2.get("max_ts")
    interval = _interval_for(dataset)
    if min_ts and max_ts and interval:
        try:
            span = (datetime.fromisoformat(max_ts)
                    - datetime.fromisoformat(min_ts)).total_seconds()
            expected = int(span) // interval + 1
        except (TypeError, ValueError):
            expected = total
    missing = max(0, expected - total)
    completeness = 1.0 if expected <= 0 else 1.0 - missing / expected

    # ---- validity ----
    validity = 1.0 if total <= 0 else 1.0 - invalid_total / total

    # ---- consistency（跨源） ----
    max_ratio = 0.0
    cross = level3.get("cross_source_volume") or cross_source_volume
    if isinstance(cross, dict):
        max_ratio = float(cross.get("max_ratio", 0.0) or 0.0)
    consistency = 1.0 - max_ratio

    # ---- freshness ----
    freshness = 1.0
    if max_ts:
        defn = _safe_definition(dataset, "")
        expected_fresh = (int(defn["expected_freshness_sec"])
                          if defn and defn.get("expected_freshness_sec") else 3600)
        try:
            lag = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(max_ts)).total_seconds()
            freshness = _clamp01(1.0 - lag / expected_fresh)
        except (TypeError, ValueError):
            freshness = 1.0

    score = (0.4 * completeness + 0.3 * validity
             + 0.2 * consistency + 0.1 * freshness) * 100
    return {
        "data_type": data_type,
        "total": total,
        "expected": expected,
        "missing": missing,
        "duplicate": duplicate,
        "nulls": nulls,
        "invalid": invalid,
        "regression": regression,
        "cross_source_max_ratio": max_ratio,
        "completeness": round(completeness, 4),
        "validity": round(validity, 4),
        "consistency": round(consistency, 4),
        "freshness": round(freshness, 4),
        "score": round(score, 1),
    }


def score_report(report: dict) -> Dict:
    """整份报告 → 各 data_type 评分 + overall（instruments 不参与）"""
    scores = {}
    for r in report.get("reports", []):
        dataset = REPORT_DATASET_MAP.get(r.get("data_type"))
        if dataset is None:
            continue
        scores[dataset] = score_data_type(
            r, dataset,
            cross_source_volume=report.get("cross_source_volume"),
        )
    if scores:
        overall = round(
            sum(v["score"] for v in scores.values()) / len(scores), 1
        )
    else:
        overall = 100.0
    return {"scores": scores, "overall": overall}


# ------------------------------------------------------------------
# on_success：写 data_asset_state
# ------------------------------------------------------------------
def apply_quality_report(report_path: str, store=None, job: dict = None) -> Optional[dict]:
    """QUALITY_CHECK 完成后：解析报告 → 评分 → 写 data_asset_state"""
    import os

    if not report_path or not os.path.exists(report_path):
        logger.warning("质量报告文件缺失，跳过评分写回: %s", report_path)
        return None
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            report = json.load(f)
    except (OSError, ValueError) as e:
        logger.error("质量报告解析失败: %s | %s", report_path, e)
        return None

    result = score_report(report)
    inst_id = report.get("inst_id")
    report_bar = report.get("bar")
    if inst_id:
        _write_scores(inst_id, report_bar, result["scores"], report)
    return result


def _write_scores(inst_id: str, report_bar: Optional[str],
                  scores: Dict, report: dict) -> None:
    """评分写回 data_asset_state（资产行不存在自动 upsert）"""
    from ..db.models import DataAsset, DataAssetState
    from .assets import determine_status

    now = datetime.now(timezone.utc)
    with session_scope() as s:
        for dataset, sc in scores.items():
            bar = "" if dataset in ("TRADES", "FUNDING_RATE", "OPEN_INTEREST") else (report_bar or "1D")
            asset = (
                s.query(DataAsset)
                .filter(
                    DataAsset.exchange == "OKX", DataAsset.market == "SWAP",
                    DataAsset.inst_id == inst_id, DataAsset.dataset == dataset,
                    DataAsset.bar == bar,
                )
                .first()
            )
            if asset is None:
                asset = DataAsset(
                    exchange="OKX", market="SWAP", inst_id=inst_id,
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
            state.quality_score = sc["score"]
            state.last_check_at = now
            state.checked_at = now
            if state.row_count is None or sc["total"] > state.row_count:
                state.row_count = sc["total"]
            state.detail = {
                "quality": {k: sc[k] for k in (
                    "completeness", "validity", "consistency", "freshness",
                    "missing", "duplicate", "nulls", "invalid", "regression",
                )},
                "summary": report.get("issues", [])[:20],
            }
            state.status = determine_status(
                int(state.row_count or 0),
                float(state.freshness_lag_sec) if state.freshness_lag_sec is not None else None,
                None,
                sc["score"],
            )
    if scores:
        overall = round(sum(v["score"] for v in scores.values()) / len(scores), 1)
    else:
        overall = 100.0
    logger.info("质量评分写回完成: %s | %s | overall=%.1f",
                inst_id, ", ".join(scores.keys()), overall)
