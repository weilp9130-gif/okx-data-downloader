"""REST 历史数据回填入口

支持 instruments / oi / mark / index / funding / trades / trade_aggregates。

示例：
    python backfill.py --type instruments
    python backfill.py --type mark --inst BTC-USDT-SWAP --bar 1D --start 2024-01-01 --end 2024-02-01
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

from app.config import Config
from app.database import init_db
from app.aggregation.trades import TradeAggregator
from app.downloader.funding import FundingRateDownloader
from app.downloader.index_price import IndexPriceDownloader
from app.downloader.instruments import InstrumentDownloader
from app.downloader.mark_price import MarkPriceDownloader
from app.downloader.open_interest import OpenInterestDownloader
from app.downloader.trades import TradesDownloader
from app.okx_client import OKXClient
from app.utils.logger import get_logger
from app.utils.time_utils import parse_date

logger = get_logger("backfill")

VALID_TYPES = {"instruments", "oi", "mark", "index", "funding", "trades", "trade_aggregates"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OKX 历史数据回填工具"
    )
    parser.add_argument(
        "--type",
        dest="data_type",
        required=True,
        help="数据类型，支持: " + ", ".join(sorted(VALID_TYPES)) + ", 默认 instruments",
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
        "--bar",
        dest="bar",
        default="1D",
        help="时间粒度，默认 1D",
    )
    parser.add_argument(
        "--start",
        dest="start",
        help="开始时间，如 2024-01-01",
    )
    parser.add_argument(
        "--end",
        dest="end",
        help="结束时间，如 2025-01-01",
    )
    parser.add_argument(
        "--limit-days",
        dest="limit_days",
        type=int,
        help="未指定 --end 时，取最近 N 天",
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=10,
        help="trades 下载最大页数，默认 10",
    )
    parser.add_argument(
        "--init-db-only",
        action="store_true",
        help="仅初始化数据库表结构，不执行下载",
    )
    return parser


def _resolve_time_range(args) -> tuple:
    """解析 --start / --end / --limit-days，返回 (start, end)"""
    end = parse_date(args.end) if args.end else datetime.now(timezone.utc)
    if args.start:
        start = parse_date(args.start)
    elif args.limit_days:
        start = end - timedelta(days=args.limit_days)
    else:
        start = end.replace(year=end.year - 1)
    return start, end


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.init_db_only:
        init_db()
        logger.info("数据库初始化完成")
        return 0

    if args.data_type not in VALID_TYPES:
        logger.error("不支持的 --type: %s", args.data_type)
        return 1

    init_db()
    client = OKXClient()

    if args.data_type == "instruments":
        downloader = InstrumentDownloader(client=client, cfg=Config())
        count = downloader.download(inst_type=args.inst_type)
        logger.info("Instruments 回填完成，写入/更新 %d 条", count)
        return 0

    # 以下类型需要 --inst
    if not args.inst:
        logger.error("--type=%s 需要 --inst 参数", args.data_type)
        return 1

    if args.data_type == "oi":
        downloader = OpenInterestDownloader(client=client, cfg=Config())
        count = downloader.download(inst_id=args.inst, bar=args.bar)
        logger.info("OpenInterest 回填完成，写入/更新 %d 条", count)
        return 0

    start, end = _resolve_time_range(args)

    if args.data_type == "mark":
        downloader = MarkPriceDownloader(client=client, cfg=Config())
        count = downloader.download_range(
            inst_id=args.inst, bar=args.bar, start=start, end=end
        )
        logger.info("MarkPrice 回填完成，写入/更新 %d 条", count)
        return 0

    if args.data_type == "index":
        downloader = IndexPriceDownloader(client=client, cfg=Config())
        count = downloader.download_range(
            inst_id=args.inst, bar=args.bar, start=start, end=end
        )
        logger.info("IndexPrice 回填完成，写入/更新 %d 条", count)
        return 0

    if args.data_type == "funding":
        downloader = FundingRateDownloader(client=client, cfg=Config())
        count = downloader.download_range(inst_id=args.inst, start=start, end=end)
        logger.info("FundingRate 回填完成，写入/更新 %d 条", count)
        return 0

    if args.data_type == "trades":
        downloader = TradesDownloader(client=client, cfg=Config())
        count = downloader.download_range(
            inst_id=args.inst, start=start, end=end, max_pages=args.max_pages
        )
        logger.info("Trades 回填完成，写入/更新 %d 条", count)
        return 0

    if args.data_type == "trade_aggregates":
        aggregator = TradeAggregator()
        count = aggregator.aggregate(
            inst_id=args.inst, bar=args.bar, start=start, end=end
        )
        logger.info("Trade 聚合完成，写入/更新 %d 条", count)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
