"""资产刷新 CLI（ASSET_REFRESH 任务入口）

    python -m cli.asset_refresh --scope all --mode incremental --progress-file <path>
    python -m cli.asset_refresh --scope inst --inst BTC-USDT-SWAP --mode full

先做资产引导（缺失 data_asset 自动 upsert），再按 scope 刷新：
    incremental（默认）：增量计数；latest_ts 回退自动转 full 重算
    full：全量 COUNT 重算
"""

import argparse
import sys

from app.db.database import init_db
from app.services.assets import (
    asset_guidance,
    refresh_assets_batch,
)
from app.utils.logger import get_logger
from app.utils.progress import ProgressWriter

logger = get_logger("asset_refresh")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX 数据资产刷新")
    parser.add_argument("--scope", dest="scope", default="all",
                        choices=["all", "inst"],
                        help="刷新范围：all=全部资产；inst=单个交易对")
    parser.add_argument("--inst", dest="inst", help="产品ID（scope=inst 时必须）")
    parser.add_argument("--mode", dest="mode", default="incremental",
                        choices=["incremental", "full"],
                        help="incremental=增量；full=全量重算")
    parser.add_argument("--progress-file", dest="progress_file",
                        help="任务进度 JSONL 输出路径")
    return parser


def _all_assets_count(inst: str = None) -> int:
    from app.db.models import DataAsset
    from app.db.database import session_scope

    with session_scope() as s:
        q = s.query(DataAsset)
        if inst:
            q = q.filter(DataAsset.inst_id == inst)
        return q.count()


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.scope == "inst" and not args.inst:
        logger.error("--scope inst 时必须提供 --inst")
        return 1

    init_db()
    progress = ProgressWriter(args.progress_file).open() if args.progress_file else None
    try:
        # 资产引导：缺失 data_asset 自动 upsert
        seed_count = 0
        try:
            from app.services.assets import seed_dataset_definitions
            seed_count = seed_dataset_definitions()
        except Exception as e:
            logger.warning("dataset_definition 种子失败: %s", e)
        guidance = asset_guidance()
        logger.info("资产引导完成（seed=%d 新增=%d）", seed_count, guidance)

        assets_count = _all_assets_count(args.inst)
        if progress is not None:
            progress.stage("refresh")
        total = assets_count
        if total == 0:
            logger.warning("无待刷新资产")

        def _on_progress(done, total):
            if progress is not None:
                progress.progress(
                    percent=round(done / total * 100, 1) if total else None,
                    written=done, expected=total,
                )

        result = refresh_assets_batch(
            mode=args.mode, inst_id=args.inst, on_progress=_on_progress,
        )
        if progress is not None:
            progress.done(written=result["processed"])
        if result["failed"]:
            return 1
        return 0
    finally:
        if progress is not None:
            progress.close()


if __name__ == "__main__":
    sys.exit(main())
