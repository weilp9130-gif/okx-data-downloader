"""REST 历史数据回填入口

支持 instruments / oi / mark / index / funding / trades / trade_aggregates / all。

示例：
    python backfill.py --type instruments
    python backfill.py --type mark --inst BTC-USDT-SWAP --bar 1D --start 2024-01-01 --end 2024-02-01
    python backfill.py --type all --inst BTC-USDT-SWAP --limit-days 1
    python backfill.py --type all --inst BTC-USDT-SWAP --allow-large-backfill
    python backfill.py --type trades --inst BTC-USDT-SWAP --start 2026-08-01 --end 2026-08-02 --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.aggregation.trades import TradeAggregator
from app.config import Config
from app.database import init_db
from app.downloader.funding import FundingRateDownloader
from app.downloader.index_price import IndexPriceDownloader
from app.downloader.instruments import InstrumentDownloader
from app.downloader.mark_price import MarkPriceDownloader
from app.downloader.open_interest import OpenInterestDownloader
from app.downloader.trades import TradesDownloader
from app.okx_client import OKXClient
from app.utils.logger import get_logger
from app.utils.time_utils import bar_to_seconds, parse_date

logger = get_logger("backfill")

# 单一数据类型
SINGLE_TYPES = [
    "instruments",
    "oi",
    "mark",
    "index",
    "funding",
    "trades",
    "trade_aggregates",
]

VALID_TYPES = set(SINGLE_TYPES) | {"all"}

# --type all 默认执行的类型（不含 trades，避免意外拉取海量数据）
ALL_DEFAULT_TYPES = ["instruments", "oi", "mark", "index", "funding"]

# 需要显式 --allow-large-backfill 才纳入 --type all 的类型
LARGE_TYPES = ["trades", "trade_aggregates"]

# 不需要 --inst 的类型
NO_INST_TYPES = {"instruments"}

# OKX 单页最大返回条数
PAGE_LIMIT = 100

# 资金费率结算周期（秒）
FUNDING_INTERVAL_SECONDS = 8 * 3600


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX 历史数据回填工具")
    parser.add_argument(
        "--type",
        dest="data_type",
        required=True,
        help="数据类型，支持: " + ", ".join(sorted(VALID_TYPES)),
    )
    parser.add_argument(
        "--inst-type",
        dest="inst_type",
        default="SWAP",
        help="OKX 产品类型，默认 SWAP，可选 SPOT/FUTURES/OPTION/MARGIN",
    )
    parser.add_argument(
        "--inst",
        dest="inst",
        help="产品ID，如 BTC-USDT-SWAP / BTC-USDT",
    )
    parser.add_argument(
        "--index-inst",
        dest="index_inst",
        help="指数ID，如 BTC-USDT。未指定时从 --inst 推导（去掉 -SWAP/-FUTURES 后缀）",
    )
    parser.add_argument(
        "--bar",
        dest="bar",
        default="1D",
        help="时间粒度，默认 1D",
    )
    parser.add_argument(
        "--start",
        dest="start",
        help="开始时间，如 2024-01-01（优先级最高）",
    )
    parser.add_argument(
        "--end",
        dest="end",
        help="结束时间，如 2025-01-01（优先级最高）",
    )
    parser.add_argument(
        "--limit-days",
        dest="limit_days",
        type=int,
        help="未指定 --start 时，取最近 N 天（end=now, start=now-N天）",
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=10,
        help="trades 下载最大页数，默认 10",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="只估算请求次数/数据量，不写库",
    )
    parser.add_argument(
        "--allow-large-backfill",
        dest="allow_large",
        action="store_true",
        help="--type all 时纳入 trades / trade_aggregates 等大数据量类型",
    )
    parser.add_argument(
        "--init-db-only",
        action="store_true",
        help="仅初始化数据库表结构，不执行下载",
    )
    return parser


def _resolve_time_range(args) -> tuple:
    """解析 --start / --end / --limit-days，返回 (start, end)

    优先级：--start/--end > --limit-days > 默认最近一年。
    CLI 统一解析，各 Downloader 不再各自解释 --limit-days。
    """
    end = parse_date(args.end) if args.end else datetime.now(timezone.utc)
    if args.start:
        start = parse_date(args.start)
    elif args.limit_days:
        start = end - timedelta(days=args.limit_days)
    else:
        start = end.replace(year=end.year - 1)
    return start, end


def _resolve_index_inst(args) -> str:
    """解析指数 instId

    index-candles 只接受指数ID（如 BTC-USDT），不接受 BTC-USDT-SWAP。
    未显式指定时从 --inst 去掉合约后缀推导。
    """
    if args.index_inst:
        return args.index_inst
    inst = args.inst or ""
    for suffix in ("-SWAP", "-FUTURES"):
        if inst.endswith(suffix):
            return inst[: -len(suffix)]
    return inst


def _resolve_types(args) -> List[str]:
    """解析 --type，展开 all"""
    if args.data_type != "all":
        return [args.data_type]
    types = list(ALL_DEFAULT_TYPES)
    if args.allow_large:
        types.extend(LARGE_TYPES)
    else:
        logger.info(
            "--type all 默认跳过 %s，需要 --allow-large-backfill 才执行",
            ", ".join(LARGE_TYPES),
        )
    return types


def _estimate(data_type: str, args, start: datetime, end: datetime) -> dict:
    """估算请求次数与数据量（--dry-run 用，不发起请求）"""
    span_seconds = max((end - start).total_seconds(), 0)

    if data_type == "instruments":
        return {"requests": 1, "rows": "~500 (按 inst_type 全量)", "span": "N/A"}

    if data_type == "oi":
        return {
            "requests": 1,
            "rows": 1,
            "span": "当前快照（OKX 无历史 OI 端点）",
        }

    if data_type in ("mark", "index"):
        try:
            interval = bar_to_seconds(args.bar)
        except ValueError:
            interval = 86400
        rows = int(span_seconds // interval) if interval else 0
        requests = max(1, -(-rows // PAGE_LIMIT))
        return {"requests": requests, "rows": rows, "span": f"{span_seconds / 86400:.1f} 天"}

    if data_type == "funding":
        rows = int(span_seconds // FUNDING_INTERVAL_SECONDS)
        requests = max(1, -(-rows // PAGE_LIMIT))
        return {"requests": requests, "rows": rows, "span": f"{span_seconds / 86400:.1f} 天"}

    if data_type == "trades":
        requests = args.max_pages
        return {
            "requests": requests,
            "rows": requests * PAGE_LIMIT,
            "span": f"最多 {requests} 页（受 --max-pages 限制）",
        }

    if data_type == "trade_aggregates":
        try:
            interval = bar_to_seconds(args.bar)
        except ValueError:
            interval = 60
        buckets = int(span_seconds // interval) if interval else 0
        return {"requests": 0, "rows": buckets, "span": f"{span_seconds / 86400:.1f} 天（本地聚合）"}

    return {"requests": "?", "rows": "?", "span": "?"}


def _print_dry_run(types: List[str], args, start: datetime, end: datetime) -> None:
    logger.info("=== DRY RUN（不写库）===")
    logger.info("inst=%s | bar=%s | %s ~ %s", args.inst, args.bar, start.isoformat(), end.isoformat())
    total_requests = 0
    for t in types:
        est = _estimate(t, args, start, end)
        if isinstance(est["requests"], int):
            total_requests += est["requests"]
        logger.info(
            "  %-18s requests=%-6s rows=%-10s span=%s",
            t, est["requests"], est["rows"], est["span"],
        )
    logger.info("预计 REST 请求总数: %s", total_requests)


def _run_one(data_type: str, args, client: OKXClient,
             start: datetime, end: datetime) -> int:
    """执行单个数据类型的回填，返回写入行数"""
    cfg = Config()

    if data_type == "instruments":
        count = InstrumentDownloader(client=client, cfg=cfg).download(
            inst_type=args.inst_type
        )
        logger.info("Instruments 回填完成，写入/更新 %d 条", count)
        return count

    if data_type == "oi":
        count = OpenInterestDownloader(client=client, cfg=cfg).download(
            inst_id=args.inst, bar=args.bar
        )
        logger.info("OpenInterest 回填完成，写入/更新 %d 条", count)
        return count

    if data_type == "mark":
        count = MarkPriceDownloader(client=client, cfg=cfg).download_range(
            inst_id=args.inst, bar=args.bar, start=start, end=end
        )
        logger.info("MarkPrice 回填完成，写入/更新 %d 条", count)
        return count

    if data_type == "index":
        index_inst = _resolve_index_inst(args)
        count = IndexPriceDownloader(client=client, cfg=cfg).download_range(
            inst_id=index_inst, bar=args.bar, start=start, end=end
        )
        logger.info("IndexPrice 回填完成: %s，写入/更新 %d 条", index_inst, count)
        return count

    if data_type == "funding":
        count = FundingRateDownloader(client=client, cfg=cfg).download_range(
            inst_id=args.inst, start=start, end=end
        )
        logger.info("FundingRate 回填完成，写入/更新 %d 条", count)
        return count

    if data_type == "trades":
        count = TradesDownloader(client=client, cfg=cfg).download_range(
            inst_id=args.inst, start=start, end=end, max_pages=args.max_pages
        )
        logger.info("Trades 回填完成，写入/更新 %d 条", count)
        return count

    if data_type == "trade_aggregates":
        count = TradeAggregator().aggregate(
            inst_id=args.inst, bar=args.bar, start=start, end=end
        )
        logger.info("Trade 聚合完成，写入/更新 %d 条", count)
        return count

    logger.error("不支持的类型: %s", data_type)
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.init_db_only:
        init_db()
        logger.info("数据库初始化完成")
        return 0

    if args.data_type not in VALID_TYPES:
        logger.error("不支持的 --type: %s（可选: %s）",
                     args.data_type, ", ".join(sorted(VALID_TYPES)))
        return 1

    types = _resolve_types(args)
    start, end = _resolve_time_range(args)

    # 校验 --inst
    needs_inst = [t for t in types if t not in NO_INST_TYPES]
    if needs_inst and not args.inst:
        logger.error("类型 %s 需要 --inst 参数", ", ".join(needs_inst))
        return 1

    if args.dry_run:
        _print_dry_run(types, args, start, end)
        return 0

    init_db()
    client = OKXClient()

    failures = []
    for t in types:
        try:
            _run_one(t, args, client, start, end)
        except Exception as e:
            failures.append((t, str(e)))
            logger.error("回填失败: %s | %s", t, e)

    if failures:
        logger.error("以下类型回填失败: %s", ", ".join(t for t, _ in failures))
        return 1

    if len(types) > 1:
        logger.info("全部回填完成: %s", ", ".join(types))
    return 0


if __name__ == "__main__":
    sys.exit(main())
