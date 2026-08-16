"""数据质量报告入口

示例：
    python quality_report.py --type mark --inst BTC-USDT-SWAP --bar 1D
    python quality_report.py --type trades --inst BTC-USDT-SWAP
    python quality_report.py --type all --inst BTC-USDT-SWAP --bar 1D
    python quality_report.py --type all --inst BTC-USDT-SWAP --explain-index --fail-on-issue
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from app.database import init_db
from app.quality.validator import DataQualityValidator
from app.utils.logger import get_logger
from app.utils.time_utils import parse_date

logger = get_logger("quality_report")

ALL_TYPES = ["instruments", "candles", "funding", "mark", "index", "oi", "trades"]

# 需要 bar 参数的类型
BAR_TYPES = ("candles", "mark", "index")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX 数据质量报告工具")
    parser.add_argument(
        "--type",
        dest="data_type",
        required=True,
        help="数据类型: " + "/".join(ALL_TYPES) + "/all",
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
        help="执行 Level 3 跨源一致性校验（trades vs candles volume）",
    )
    parser.add_argument(
        "--explain-index",
        dest="explain_index",
        action="store_true",
        help="用 EXPLAIN ANALYZE 验证 (inst_id, ts) 查询是否走索引",
    )
    parser.add_argument(
        "--fail-on-issue",
        dest="fail_on_issue",
        action="store_true",
        help="发现 duplicate/null/invalid/时间倒序等问题时退出码返回 1（回归测试用）",
    )
    parser.add_argument(
        "--output",
        dest="output",
        help="报告输出 JSON 文件路径",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    init_db()

    types = ALL_TYPES if args.data_type == "all" else [args.data_type]

    validator = DataQualityValidator()
    reports = []
    issues = []
    errors = []

    for t in types:
        try:
            bar = args.bar if t in BAR_TYPES else None
            report = validator.validate(t, args.inst, bar)
            reports.append(report)
            issues.extend(validator.collect_issues(report))
        except Exception as e:
            errors.append(f"{t}: {e}")
            logger.error("验证失败: %s | %s | %s", t, args.inst, e)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inst_id": args.inst,
        "bar": args.bar,
        "reports": reports,
        "issues": issues,
        "errors": errors,
    }

    if args.cross_source and "trades" in types:
        start = parse_date(args.start) if args.start else (
            datetime.now(timezone.utc).replace(year=2024, month=1, day=1)
        )
        end = parse_date(args.end) if args.end else datetime.now(timezone.utc)
        result["cross_source_volume"] = validator.cross_source_volume_check(
            args.inst, args.bar, start, end
        )

    if args.explain_index:
        index_check = validator.explain_index_usage(args.inst)
        result["index_check"] = {
            table: {
                "uses_index": info.get("uses_index"),
                "seq_scan": info.get("seq_scan"),
                "rows": info.get("rows"),
                "skipped": info.get("skipped"),
                "error": info.get("error"),
            }
            for table, info in index_check.items()
        }
        result["index_check_plans"] = {
            table: info.get("plan") for table, info in index_check.items()
        }
        for table, info in index_check.items():
            if info.get("error"):
                issues.append(f"index_check/{table}/error={info['error']}")
            elif not info.get("uses_index") and not info.get("skipped"):
                issues.append(
                    f"index_check/{table}/seq_scan_on_{info.get('rows')}_rows"
                )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, default=str, indent=2)
        logger.info("报告已保存至 %s", args.output)
    else:
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))

    if issues:
        logger.warning("发现 %d 个数据质量问题:", len(issues))
        for issue in issues:
            logger.warning("  %s", issue)
    else:
        logger.info("数据质量检查通过，无 duplicate/null/invalid 问题")

    if args.fail_on_issue and (issues or errors):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
