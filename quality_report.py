"""数据质量报告入口

示例：
    python quality_report.py --type mark --inst BTC-USDT-SWAP --bar 1D
    python quality_report.py --type trades --inst BTC-USDT-SWAP
    python quality_report.py --type all --inst BTC-USDT-SWAP --bar 1D
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from app.config import Config
from app.database import init_db
from app.quality.validator import DataQualityValidator
from app.utils.logger import get_logger
from app.utils.time_utils import parse_date

logger = get_logger("quality_report")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX 数据质量报告工具")
    parser.add_argument(
        "--type",
        dest="data_type",
        required=True,
        help="数据类型: instruments/candles/funding/mark/index/oi/trades/all",
    )
    parser.add_argument("--inst", dest="inst", required=True, help="产品ID")
    parser.add_argument("--bar", dest="bar", default="1D", help="时间粒度，默认 1D")
    parser.add_argument(
        "--start",
        dest="start",
        help="跨源校验开始时间，如 2024-01-01",
    )
    parser.add_argument(
        "--end",
        dest="end",
        help="跨源校验结束时间，如 2024-02-01",
    )
    parser.add_argument(
        "--cross-source",
        dest="cross_source",
        action="store_true",
        help="是否执行 Level 3 跨源一致性校验",
    )
    parser.add_argument(
        "--output",
        dest="output",
        help="报告输出 JSON 文件路径",
    )
    return parser


def run_validate(data_type: str, inst_id: str, bar: str) -> dict:
    v = DataQualityValidator()
    return v.validate(data_type, inst_id, bar)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    init_db()

    if args.data_type == "all":
        types = ["instruments", "candles", "funding", "mark", "index", "oi", "trades"]
    else:
        types = [args.data_type]

    reports = []
    for t in types:
        try:
            bar = args.bar if t in ("candles", "mark", "index") else None
            report = run_validate(t, args.inst, bar)
            reports.append(report)
        except Exception as e:
            logger.error("验证失败: %s | %s | %s", t, args.inst, e)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inst_id": args.inst,
        "bar": args.bar,
        "reports": reports,
    }

    if args.cross_source and "trades" in types:
        v = DataQualityValidator()
        start = parse_date(args.start) if args.start else datetime.now(timezone.utc).replace(year=2024, month=1, day=1)
        end = parse_date(args.end) if args.end else datetime.now(timezone.utc)
        cross = v.cross_source_volume_check(args.inst, args.bar, start, end)
        result["cross_source_volume"] = cross

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, default=str, indent=2)
        logger.info("报告已保存至 %s", args.output)
    else:
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
