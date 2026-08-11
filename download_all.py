"""OKX 永续合约 K线 全历史 并行下载脚本（每合约回溯版）

需求：对每个合约，从"当前时间（或指定的 end）"一路往回下载，
直到没有数据为止（即回溯到该合约实际上线时间，OKX 返回空即停）。

实现方式：
- 每个合约一个任务，调用 download_range(start=None, end=end, force_full_range=True)
- download_range 内部用 OKX history-candles 的 after 参数从最新往回回溯，
  直到 oldest_ts <= 窗口起点 或 OKX 返回空（代表无更早数据）为止。
- 多合约 ThreadPoolExecutor 并行
- 幂等写入（ON CONFLICT DO NOTHING），重跑不重复入库
- 全局请求限速 + 429 自适应降速（OKXClient 内）+ thread-local Session

用法：
    python download_all.py                                  # 全历史 1m
    python download_all.py --workers 8 --bar 5m             # 自定义并发/粒度
    python download_all.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
"""

import argparse
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from config import Config
from utils.logger import setup_logging, get_logger
from database import init_db, dispose_engine
from okx_client import OKXClient
from downloader.candles import CandleDownloader

logger = get_logger(__name__)

_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到中断信号，正在安全退出（已入库数据不丢失）...')


def parse_args():
    p = argparse.ArgumentParser(description='OKX 永续合约全历史并行下载（每合约回溯）')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--workers', type=int, default=8, help='并行下载数，默认8')
    p.add_argument('--end', default=None, help='结束日期(UTC)，默认当前时间')
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


def main():
    args = parse_args()
    cfg = Config()

    setup_logging(level=args.log_level or cfg.logging.level,
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

        tasks = [(c['instId'], bar, c.get('listTime'), end) for c in contracts]

        logger.info('=' * 60)
        logger.info(f'全历史并行下载（每合约回溯）| {bar} | 至 {end:%Y-%m-%d}')
        logger.info(f'合约 {len(contracts)} 个 | 并发 {args.workers} '
                    f'| 每合约从上线时间(或2019)回溯到现在/end')
        logger.info('=' * 60)

        ok = fail = 0
        total_rows = 0

        def work(task):
            inst, b, list_time, c_end = task
            if _shutdown:
                return (inst, 0, 'shutdown')
            c_start = None
            if list_time:
                lt = ms_to_dt(int(list_time))
                if lt:
                    c_start = lt
            try:
                # start=None + force_full_range=True：
                # 从 c_end 一路往回回溯到实际上线时间（listTime/2019），
                # 不因库内已有近期数据而干扰窗口语义，直到无数据自动停止。
                n = candle_dl.download_range(
                    inst, b, start=c_start, end=c_end, force_full_range=True)
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
