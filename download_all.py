"""OKX 永续合约 K线 全历史 并行下载脚本（性能优化版）

参考 OKX 官方多线程同步脚本，结合本项目的 PostgreSQL/TimescaleDB 架构。

核心优化（解决"全历史下载慢 + 新币无效回溯"）：
1. listTime 下界：每个合约只回溯到它的实际上线时间
   （start = max(用户start, 合约listTime)，避免 2023~2026 上线的
    190+ 个新币从 2019 开始做大量无效回溯）
2. 合约 x 时间窗 并行：把 [start,end] 切成 N 天一块的窗口，
   任务 = 合约 x 窗口，用 ThreadPoolExecutor 并行拉满全局限速
3. 线程本地 Session（thread-local）：OKXClient 内已实现，避免多线程共享
   requests.Session 竞争，连接复用更高效
4. 全局请求限速（OKXClient 内）+ 429 自适应降速，稳定不触发频控
5. 窗口级增量续传：已下载的时间窗自动跳过，Ctrl+C 重跑不重复

用法：
    python download_all.py                                  # 全历史 1m
    python download_all.py --workers 8 --bar 5m             # 自定义并发/粒度
    python download_all.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP --start 2020-01-01
    python download_all.py --days-per-window 7              # 每时间窗 7 天
"""

import argparse
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from config import Config
from utils.logger import setup_logging, get_logger
from database import init_db, dispose_engine, get_engine
from okx_client import OKXClient
from downloader.candles import CandleDownloader

logger = get_logger(__name__)

_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到中断信号，正在安全退出（已入库数据不丢失）...')


def parse_args():
    p = argparse.ArgumentParser(description='OKX 永续合约全历史并行下载')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--workers', type=int, default=8, help='并行下载数，默认8')
    p.add_argument('--start', default=None,
                   help='开始日期(UTC)如2023-01-01，默认2019(最早)')
    p.add_argument('--end', default=None, help='结束日期(UTC)，默认今天')
    p.add_argument('--days-per-window', type=int, default=30,
                   help='每个时间窗天数，默认30')
    p.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                   default=None, help='日志级别')
    return p.parse_args()


def parse_date(s):
    if not s:
        return None
    return (datetime.strptime(s.strip(), '%Y-%m-%d')
            .replace(tzinfo=timezone.utc).replace(tzinfo=None))


def ms_to_dt(ms):
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def get_swap_contracts(client):
    """返回带 listTime 的 USDT 永续合约列表（仅 state=live）"""
    data = client.get_instruments('SWAP')
    contracts = []
    for d in data:
        if d.get('settleCcy') == 'USDT' and d.get('state') == 'live':
            contracts.append({'instId': d['instId'], 'listTime': d.get('listTime')})
    contracts.sort(key=lambda x: x['instId'])
    return contracts


def windows_covered(inst_id, bar, ss, se):
    """查询该 (合约, 时间窗) 在主表 candles 中是否已有数据（用于续传跳过）"""
    try:
        with get_engine().connect() as conn:
            cnt = conn.execute(
                text("SELECT COUNT(*) FROM candles "
                     "WHERE inst_id=:i AND bar=:b AND ts>=:a AND ts<:z"),
                {"i": inst_id, "b": bar,
                 "a": ss.replace(tzinfo=timezone.utc),
                 "z": se.replace(tzinfo=timezone.utc)},
            ).scalar()
        return bool(cnt)
    except Exception:
        return False


def split_windows(start, end, days):
    step = timedelta(days=days)
    windows = []
    cur = start
    while cur < end:
        nxt = min(cur + step, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows


def main():
    args = parse_args()
    cfg = Config()

    setup_logging(level=args.log_level or cfg.logging.level,
                  file_enabled=cfg.logging.file_enabled,
                  max_bytes=cfg.logging.max_bytes,
                  backup_count=cfg.logging.backup_count)

    end = parse_date(args.end) or datetime.now(timezone.utc).replace(tzinfo=None)
    cli_start = parse_date(args.start) or datetime(2019, 1, 1)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    client = None
    try:
        init_db()
        client = OKXClient()
        candle_dl = CandleDownloader(client, cfg)
        bar = args.bar

        if args.insts:
            contracts = [{'instId': i.strip(), 'listTime': None}
                         for i in args.insts.split(',') if i.strip()]
        else:
            contracts = get_swap_contracts(client)

        if not contracts:
            logger.error('没有可下载的合约')
            return

        # 用 listTime 规定每个合约的回溯下界（关键优化）
        tasks = []
        for c in contracts:
            inst = c['instId']
            c_start = cli_start
            if c.get('listTime'):
                lt = ms_to_dt(int(c['listTime']))
                if lt and lt > c_start:
                    c_start = lt
            if c_start >= end:
                continue
            for ss, se in split_windows(c_start, end, args.days_per_window):
                tasks.append((inst, bar, ss, se))

        tasks.sort(key=lambda t: t[2])  # 按起始时间排序，先下旧的
        logger.info('=' * 60)
        logger.info(f'全历史并行下载 | {bar} | {cli_start:%Y-%m-%d} ~ {end:%Y-%m-%d}')
        logger.info(f'合约 {len(contracts)} 个 | 时间窗 {args.days_per_window}天 '
                    f'| 并发 {args.workers} | 总任务 {len(tasks)} 个')
        logger.info('=' * 60)

        ok = skipped = fail = 0
        total_rows = 0

        def work(task):
            inst, b, ss, se = task
            if _shutdown:
                return (inst, 0, 'shutdown')
            # 窗口级增量续传
            if windows_covered(inst, b, ss, se):
                return (inst, 0, 'skipped')
            try:
                n = candle_dl.download_range(inst, b, ss, se)
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
                if err == 'skipped':
                    skipped += 1
                elif err:
                    fail += 1
                    logger.error(f'{inst}: {err}')
                else:
                    ok += 1
                    total_rows += n
                if done % 100 == 0:
                    logger.info(f'[进度] 已完成 {done}/{len(tasks)} '
                                f'(跳过{skipped} 成功{ok} 失败{fail})')

        logger.info('-' * 60)
        logger.info(f'完成: 成功 {ok} | 跳过已下载 {skipped} | 失败 {fail} | '
                    f'累计写入 {total_rows} 根')

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
