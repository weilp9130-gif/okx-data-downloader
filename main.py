"""
OKX数据下载器 - 程序入口

用法：
    python main.py                          # 使用默认配置下载
    python main.py --inst ETH-USDT-SWAP     # 指定交易对
    python main.py --bar 4H                 # 指定时间粒度
    python main.py --start 2024-01-01 --end 2025-12-31
    python main.py --type funding           # 仅下载资金费率
    python main.py --update                 # 增量更新最近数据
"""

import argparse
import sys
from datetime import datetime, timedelta

from app.config import Config
from app.utils.logger import setup_logging, get_logger
from app.utils.time_utils import parse_date, utc_now
from app.database import init_db, dispose_engine
from app.okx_client import OKXClient
from app.proxy_pool import build_proxy_pool
from app.downloader.candles import CandleDownloader
from app.downloader.funding import FundingRateDownloader

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="OKX行情数据下载器 (PostgreSQL + TimescaleDB)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--inst", "-i", default=None,
                        help="交易对/产品ID，例如 ETH-USDT-SWAP 或 BTC-USDT")
    parser.add_argument("--bar", "-b", default=None, help="K线粒度，如 1m, 15m, 4H, 1D")
    parser.add_argument("--start", default=None, help="开始日期，如 2024-01-01 (UTC)")
    parser.add_argument("--end", default=None, help="结束日期，如 2025-12-31 (UTC)")
    parser.add_argument("--type", "-t", choices=["candles", "funding", "both"],
                        default="both", help="下载数据类型")
    parser.add_argument("--update", action="store_true",
                        help="增量更新模式：只拉取最近N天的数据")
    parser.add_argument("--lookback", type=int, default=7,
                        help="增量更新模式下的回看天数（默认7天）")
    parser.add_argument("--log-level",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default=None, help="覆盖默认日志级别")
    parser.add_argument("--init-db-only", action="store_true",
                        help="仅初始化数据库表结构，不执行下载")
    parser.add_argument("--proxy-pool", action="store_true",
                        help="启用IP代理池（需配置OKX_PROXY_URLS，每币一个IP）")
    parser.add_argument("--proxy-verify", action="store_true",
                        help="启动时探测代理池各代理出口IP并统计独立IP数")
    parser.add_argument("--per-ip-rate", type=int, default=None,
                        help="每个IP的请求限速（默认读配置OKX_IP_RATE_LIMIT_PER_SECOND）")
    parser.add_argument("--dynamic", action="store_true",
                        help="动态IP池：每次下载前自动发现节点/测IP/应用listeners，"
                             "兼容节点与IP变化的VPN服务商（需Clash/Mihomo运行中）")
    parser.add_argument("--pool-size", type=int, default=20,
                        help="动态IP池的独立IP数量上限，默认20")
    parser.add_argument("--pool-ttl", type=int, default=0,
                        help="复用节点IP缓存秒数，0=每次重测（默认）")
    parser.add_argument("--pool-base-port", type=int, default=7891,
                        help="动态IP池监听起始端口，默认7891")
    parser.add_argument("--no-prompt", action="store_true",
                        help="动态模式等待端口就绪超时后不交互提示，直接报错")

    return parser.parse_args()


def main() -> None:
    """主函数"""
    args = parse_args()
    config = Config()

    # 初始化日志
    setup_logging(
        name="main",
        level=args.log_level or config.logging.level,
        file_enabled=config.logging.file_enabled,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )

    try:
        # 初始化数据库
        logger.info("初始化数据库连接...")
        init_db()

        if args.init_db_only:
            logger.info("数据库初始化完成，程序退出。")
            return

        # 初始化OKX客户端（可选IP代理池：一个币一个IP并行下载）
        pool = build_proxy_pool(args)
        client = OKXClient(proxy_pool=pool)
        if pool is not None:
            logger.info(f"代理池模式: {pool.stats()}")

        # 解析交易范围
        start, end = _resolve_date_range(args)
        logger.info(f"数据时间范围: {start} ~ {end}")

        # 解析交易对列表
        inst_list = _resolve_instruments(args, config)

        # 执行下载
        if args.type in ("both", "candles"):
            bar = args.bar or config.download.default_bar
            _download_candles(client, config, inst_list, bar, start, end, args)

        if args.type in ("both", "funding"):
            _download_funding(client, config, inst_list, start, end, args)

        logger.info("全部任务执行完成。")

    except KeyboardInterrupt:
        logger.warning("用户中断程序")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"程序执行出错: {e}")
        sys.exit(1)
    finally:
        dispose_engine()


def _resolve_date_range(args) -> tuple:
    """解析开始/结束日期"""
    now = utc_now().replace(tzinfo=None)

    if args.update:
        start = now - timedelta(days=args.lookback)
        end = now
    else:
        # 默认下载全部历史（OKX约2019年底上线）
        start = parse_date(args.start) if args.start else datetime(2019, 1, 1)
        end = parse_date(args.end) if args.end else now
        if start >= end:
            raise ValueError(f"开始日期 {start} 不能在结束日期 {end} 之后")
    return start, end


def _resolve_instruments(args, config: Config) -> list:
    """解析需要下载的交易对列表"""
    if args.inst:
        return [args.inst]
    return config.download.spot_instruments + config.download.swap_instruments


def _download_candles(client, config, inst_list, bar, start, end, args) -> None:
    """下载K线数据"""
    downloader = CandleDownloader(client, config)
    for inst_id in inst_list:
        logger.info(f"==== 下载K线: {inst_id} | {bar} ====")
        try:
            if args.update:
                count = downloader.update_latest(inst_id, bar,
                                                 lookback_days=args.lookback)
            else:
                count = downloader.download_range(inst_id, bar, start, end)
            logger.info(f"{inst_id} | {bar} 下载完成，写入 {count} 根")
        except Exception as e:
            logger.error(f"下载 {inst_id} K线失败: {e}")


def _download_funding(client, config, inst_list, start, end, args) -> None:
    """下载资金费率（仅合约）"""
    downloader = FundingRateDownloader(client, config)
    swap_list = [i for i in inst_list if i.endswith("-SWAP")]
    if not swap_list:
        logger.info("当前交易对列表无合约产品，跳过资金费率下载")
        return
    for inst_id in swap_list:
        logger.info(f"==== 下载资金费率: {inst_id} ====")
        try:
            if args.update:
                count = downloader.update_latest(inst_id,
                                                 lookback_days=args.lookback)
            else:
                count = downloader.download_range(inst_id, start, end)
            logger.info(f"{inst_id} 资金费率下载完成，写入 {count} 条")
        except Exception as e:
            logger.error(f"下载 {inst_id} 资金费率失败: {e}")


if __name__ == "__main__":
    main()