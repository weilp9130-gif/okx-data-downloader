"""OKX 永续合约 K线 全历史 并行下载脚本（每合约回溯版）

需求：对每个合约，从"当前时间（或指定的 end）"一路往回下载，
直到没有数据为止（即回溯到该合约实际上线时间，OKX 返回空即停）。

实现方式：
- 每个合约一个任务，调用 download_range(start=listTime, end=end)
- download_range 内部先查库：按天比对已有条数，只对缺失窗口发起下载，
  已完整的数据直接跳过，绝不重复下载
- 缺失窗口用 OKX history-candles 的 after 参数从最新往回回溯补下，
  直到窗口起点或 OKX 返回空（代表无更早数据）为止。
- 多合约 ThreadPoolExecutor 并行
- 幂等写入（ON CONFLICT DO NOTHING），重跑不重复入库
- 全局请求限速 + 429 自适应降速（OKXClient 内）+ thread-local Session
- 可选 IP 代理池：--proxy-pool 时为每个币绑定一个独立出口IP，
  每个IP独立限速，吞吐 ≈ IP数 × 15 req/s（打破单IP限频瓶颈）

用法：
    python download_all.py                                  # 全历史 1m
    python download_all.py --workers 8 --bar 5m             # 自定义并发/粒度
    python download_all.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
    python download_all.py --proxy-pool --proxy-verify --workers 20  # 使用IP代理池
"""

import argparse
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from app.config import Config
from app.utils.logger import setup_logging, get_logger
from app.utils.time_utils import parse_date as _parse_date
from app.utils.okx_utils import ms_to_naive_utc, get_swap_contracts
from app.database import init_db, dispose_engine
from app.okx_client import OKXClient
from app.proxy_pool import build_proxy_pool
from app.downloader.candles import CandleDownloader

logger = get_logger(__name__)

_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到中断信号，正在安全退出（已入库数据不丢失）...')


def parse_date(s):
    """宽松日期解析：解析失败返回None（供 --end 参数使用）"""
    if not s:
        return None
    try:
        return _parse_date(s)
    except ValueError:
        return None


def parse_args():
    p = argparse.ArgumentParser(description='OKX 永续合约全历史并行下载（每合约回溯）')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--workers', type=int, default=None,
                   help='并行下载数（默认8；代理池模式下自动设为 IP数×2，'
                        '每IP保持2线程以获得最大吞吐）')
    p.add_argument('--end', default=None, help='结束日期(UTC)，默认当前时间')
    p.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                   default=None, help='日志级别')
    p.add_argument('--proxy-pool', action='store_true',
                   help='启用IP代理池（需配置OKX_PROXY_URLS，每币一个IP）')
    p.add_argument('--proxy-verify', action='store_true',
                   help='启动时探测代理池各代理出口IP并统计独立IP数')
    p.add_argument('--per-ip-rate', type=int, default=None,
                   help='每个IP的请求限速（默认读配置OKX_IP_RATE_LIMIT_PER_SECOND）')
    p.add_argument('--dynamic', action='store_true',
                   help='动态IP池：每次下载前自动发现节点/测IP/应用listeners，'
                        '兼容节点与IP变化的VPN服务商（需Clash/Mihomo运行中）')
    p.add_argument('--pool-size', type=int, default=20,
                   help='动态IP池的独立IP数量上限，默认20')
    p.add_argument('--pool-ttl', type=int, default=0,
                   help='复用节点IP缓存秒数，0=每次重测（默认）')
    p.add_argument('--pool-base-port', type=int, default=7891,
                   help='动态IP池监听起始端口，默认7891')
    p.add_argument('--no-prompt', action='store_true',
                   help='动态模式等待端口就绪超时后不交互提示，直接报错')
    return p.parse_args()


def main():
    args = parse_args()
    cfg = Config()

    setup_logging(name="download_all",
                  level=args.log_level or cfg.logging.level,
                  file_enabled=cfg.logging.file_enabled,
                  max_bytes=cfg.logging.max_bytes,
                  backup_count=cfg.logging.backup_count)

    end = parse_date(args.end)
    if end is None:
        end = datetime.now(timezone.utc).replace(tzinfo=None)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    client = None
    try:
        init_db()
        pool = build_proxy_pool(args)
        client = OKXClient(proxy_pool=pool)
        candle_dl = CandleDownloader(client, cfg)
        bar = args.bar

        # 并发数默认值：代理池模式下每IP 4个并发（实测每线程~2页/s、请求延迟
        # ~450ms，需4线程才吃满每IP 8/s限速；写库已改为每批短连接，不受DB池限制）
        if args.workers is None:
            if pool is not None and len(pool) > 0:
                args.workers = max(2, min(4 * len(pool), 96))
            else:
                args.workers = 8

        if args.insts:
            contracts = [{'instId': i.strip(), 'listTime': None}
                         for i in args.insts.split(',') if i.strip()]
        else:
            contracts = get_swap_contracts(client)

        if not contracts:
            logger.error('没有可下载的合约')
            return

        tasks = [(c['instId'], bar, c.get('listTime'), end) for c in contracts]

        logger.info('=' * 60)
        logger.info(f'全历史并行下载（每合约回溯）| {bar} | 至 {end:%Y-%m-%d}')
        logger.info(f'合约 {len(contracts)} 个 | 并发 {args.workers} '
                    f'| 每合约从上线时间(或2019)回溯到现在/end')
        if pool is not None:
            logger.info(f'代理池模式: {pool.stats()}')
        logger.info('=' * 60)

        ok = fail = 0
        total_rows = 0

        def work(task):
            inst, b, list_time, c_end = task
            if _shutdown:
                return (inst, 0, 'shutdown')
            c_start = None
            if list_time:
                lt = ms_to_naive_utc(int(list_time))
                if lt:
                    c_start = lt
            try:
                # start=listTime(或None→2019)，让 download_range 先查库：
                # 已完整的数据直接跳过，只补下缺失窗口，不重复下载。
                # list_time 用于上市首日的精确判定，防止头部截断被漏检。
                n = candle_dl.download_range(
                    inst, b, start=c_start, end=c_end, list_time=c_start)
                return (inst, n, None)
            except Exception as e:
                return (inst, 0, str(e))

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(work, t): t for t in tasks}
            done = 0
            for fut in as_completed(futures):
                inst, n, err = fut.result()
                done += 1
                if err == 'shutdown':
                    logger.info('收到退出信号，停止')
                    break
                if err:
                    fail += 1
                    logger.error(f'{inst}: {err}')
                else:
                    ok += 1
                    total_rows += n
                if done % 20 == 0:
                    logger.info(f'[进度] 已完成 {done}/{len(tasks)} 合约 '
                                f'(成功{ok} 失败{fail}) 累计写入 {total_rows} 根')

        logger.info('-' * 60)
        logger.info(f'完成: 成功 {ok} | 失败 {fail} | 累计写入 {total_rows} 根')

    except KeyboardInterrupt:
        logger.warning('用户中断')
        sys.exit(130)
    except Exception as e:
        logger.exception(f'程序异常: {e}')
        sys.exit(1)
    finally:
        if client:
            client.close()
        dispose_engine()


if __name__ == '__main__':
    main()
