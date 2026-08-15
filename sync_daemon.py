"""OKX（欧易）数据 常驻实时同步守护进程

需求：系统一直运行，持续同步 OKX 的 K线 与 资金费率 到本地
PostgreSQL + TimescaleDB。

设计融合了两个方案的优点：
- 参考脚本的工程性：生产者-消费者(下载线程池 + 独立批量写库线程)、
  按K线周期对齐轮次、只拉已完结K线(confirm=="1")、增量翻页上限、
  合约定时重同步(自动发现新币)、全局限速。
- 本项目优势：PostgreSQL/TimescaleDB + SQLAlchemy ORM、
  多粒度 candles 表、资金费率 funding_rates 表、线程本地Session、
  重试/429降速/代理。

同步类型：K线(candles) + 资金费率(funding_rates)，均为多合约并行。
新币上市：周期性重同步合约列表，自动纳入同步。

用法：
    python sync_daemon.py                                   # 默认 1m + 资金费率
    python sync_daemon.py --bar 1m --workers 8              # 自定义并发
    python sync_daemon.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
    python sync_daemon.py --kline-only / --funding-only     # 只同步一种
"""

import argparse
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue, Empty

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import Config
from app.utils.logger import setup_logging, get_logger
from app.database import init_db, dispose_engine, get_engine
from app.okx_client import OKXClient
from app.models import Candle, FundingRate
from app.utils.time_utils import ms_to_datetime, bar_to_seconds

logger = get_logger(__name__)

_shutdown = False
STOP_SIGNAL = object()


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到退出信号，结束当前轮次后优雅退出...')


def parse_args():
    p = argparse.ArgumentParser(description='OKX 数据常驻实时同步守护')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--workers', type=int, default=8, help='并行下载数，默认8')
    p.add_argument('--kline-only', action='store_true', help='只同步K线')
    p.add_argument('--funding-only', action='store_true', help='只同步资金费率')
    p.add_argument('--contracts-sync-interval', type=int, default=1800,
                   help='合约列表重同步间隔(秒)，默认1800(30分钟)发现新币')
    p.add_argument('--max-incremental-pages', type=int, default=3,
                   help='增量更新每合约最多翻几页，默认3')
    p.add_argument('--round-delay', type=int, default=10,
                   help='K线收盘后额外等待秒数，默认10（避免拉到未完结K线）')
    p.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                   default=None, help='日志级别')
    return p.parse_args()


# ======================================================================
# 合约列表
# ======================================================================
def get_swap_contracts(client):
    """返回带 listTime 的 USDT 永续合约列表（仅 state=live）"""
    data = client.get_instruments('SWAP')
    contracts = []
    for d in data:
        if d.get('settleCcy') == 'USDT' and d.get('state') == 'live':
            contracts.append({'instId': d['instId'], 'listTime': d.get('listTime')})
    contracts.sort(key=lambda x: x['instId'])
    return contracts


# ======================================================================
# 轻量增量拉取
# ======================================================================
def fetch_candles_incremental(client, inst_id, bar, latest_dt, max_pages, limit=100):
    """从 OKX 拉取某合约的最新 K 线增量（只留已完结且新于库内 latest_dt）。"""
    rows = []
    after = None
    for _ in range(max_pages):
        data = client.get_candles(inst_id=inst_id, bar=bar, after=after, limit=limit)
        if not data:
            break
        hit_old = False
        for c in data:
            ts_dt = ms_to_datetime(c['ts'])
            if c.get('confirm') == '1':
                if latest_dt is not None and ts_dt <= latest_dt:
                    hit_old = True
                    continue
                rows.append({
                    'inst_id': inst_id, 'bar': bar,
                    'ts': ts_dt, 'o': c['o'], 'h': c['h'], 'l': c['l'], 'c': c['c'],
                    'vol': c['vol'], 'vol_ccy': c['vol_ccy'],
                    'vol_ccy_quote': c['vol_ccy_quote'], 'confirm': c['confirm'],
                })
        after = data[-1]['ts']
        if hit_old:
            break
    dedup = {}
    for r in rows:
        dedup[r['ts']] = r
    return [dedup[k] for k in sorted(dedup.keys())]


def fetch_funding_incremental(client, inst_id, latest_dt, max_pages, limit=100):
    rows = []
    before_ms = None
    for _ in range(max_pages):
        data = client.get_funding_rate(inst_id=inst_id, before=before_ms, limit=limit)
        if not data:
            break
        hit_old = False
        for r in data:
            ts_dt = ms_to_datetime(int(r['ts']))
            if latest_dt is not None and ts_dt <= latest_dt:
                hit_old = True
                continue
            ft = r.get('funding_time')
            rows.append({
                'inst_id': inst_id,
                'ts': ts_dt,
                'funding_rate': r.get('funding_rate'),
                'realized_rate': r.get('realized_rate'),
                'funding_time': ms_to_datetime(int(ft)) if ft else None,
            })
        before_ms = int(data[-1]['ts'])
        if hit_old:
            break
    dedup = {}
    for r in rows:
        dedup[r['ts']] = r
    return [dedup[k] for k in sorted(dedup.keys())]


# ======================================================================
# 写库
# ======================================================================
def insert_candles_batch(rows):
    if not rows:
        return 0
    stmt = pg_insert(Candle).on_conflict_do_nothing(
        constraint=Candle.__table__.primary_key)
    engine = get_engine()
    written = 0
    with engine.connect() as conn:
        for i in range(0, len(rows), 500):
            result = conn.execute(stmt, rows[i:i + 500])
            written += result.rowcount or 0
        conn.commit()
    return written


def insert_funding_batch(rows):
    if not rows:
        return 0
    stmt = pg_insert(FundingRate).on_conflict_do_nothing(
        constraint=FundingRate.__table__.primary_key)
    engine = get_engine()
    written = 0
    with engine.connect() as conn:
        for i in range(0, len(rows), 500):
            result = conn.execute(stmt, rows[i:i + 500])
            written += result.rowcount or 0
        conn.commit()
    return written


# ======================================================================
# 查询库内最新时间
# ======================================================================
def get_latest_ts_map_candles(bar):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT inst_id, MAX(ts) AS m FROM candles WHERE bar=:b "
                 "GROUP BY inst_id"), {"b": bar}).mappings().all()
    return {r["inst_id"]: r["m"] for r in rows if r["m"] is not None}


def get_latest_ts_map_funding():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT inst_id, MAX(ts) AS m FROM funding_rates "
                 "GROUP BY inst_id")).mappings().all()
    return {r["inst_id"]: r["m"] for r in rows if r["m"] is not None}


# ======================================================================
# 生产者：下载 worker
# ======================================================================
def kline_worker(client, contract, latest_map, queue, max_pages, bar):
    inst = contract['instId']
    latest = latest_map.get(inst)
    try:
        rows = fetch_candles_incremental(client, inst, bar, latest, max_pages)
        if rows:
            queue.put(('candles', rows))
        return ('kline', inst, len(rows), None)
    except Exception as e:
        return ('kline', inst, 0, str(e))


def funding_worker(client, contract, latest_map, queue, max_pages):
    inst = contract['instId']
    latest = latest_map.get(inst)
    try:
        rows = fetch_funding_incremental(client, inst, latest, max_pages)
        if rows:
            queue.put(('funding', rows))
        return ('funding', inst, len(rows), None)
    except Exception as e:
        return ('funding', inst, 0, str(e))


# ======================================================================
# 消费者：独立批量写库线程
# ======================================================================
def writer_loop(queue, db_batch=1500, flush_interval=2.0):
    batch_c = []
    batch_f = []
    last_flush_c = time.time()
    last_flush_f = time.time()
    stats = {'candles': 0, 'funding': 0, 'flushes': 0}
    while True:
        item = None
        try:
            item = queue.get(timeout=flush_interval)
        except Empty:
            pass
        now = time.time()
        if item is STOP_SIGNAL:
            if batch_c:
                stats['candles'] += insert_candles_batch(batch_c)
                batch_c.clear()
            if batch_f:
                stats['funding'] += insert_funding_batch(batch_f)
                batch_f.clear()
            stats['flushes'] += 1
            queue.task_done()
            logger.info(f'写库线程退出: K线 {stats["candles"]} | 资金费率 '
                        f'{stats["funding"]} | 刷库 {stats["flushes"]} 次')
            return
        if item is not None:
            kind, rows = item
            if kind == 'candles':
                batch_c.extend(rows)
            else:
                batch_f.extend(rows)
            queue.task_done()
        # 刷库条件：攒够数量 或 超时
        if batch_c and (len(batch_c) >= db_batch or now - last_flush_c >= flush_interval):
            stats['candles'] += insert_candles_batch(batch_c)
            last_flush_c = now
            batch_c.clear()
        if batch_f and (len(batch_f) >= db_batch or now - last_flush_f >= flush_interval):
            stats['funding'] += insert_funding_batch(batch_f)
            last_flush_f = now
            batch_f.clear()


# ======================================================================
# 跑一轮
# ======================================================================
def run_one_round(client, contracts, queue, args, do_kline, do_funding):
    latest_c = get_latest_ts_map_candles(args.bar) if do_kline else {}
    latest_f = get_latest_ts_map_funding() if do_funding else {}

    c_fetched = f_fetched = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = []
        for c in contracts:
            if do_kline:
                futs.append(ex.submit(kline_worker, client, c, latest_c,
                                      queue, args.max_incremental_pages, args.bar))
            if do_funding:
                futs.append(ex.submit(funding_worker, client, c, latest_f,
                                      queue, args.max_incremental_pages))
        for fut in as_completed(futs):
            if _shutdown:
                break
            kind, inst, n, err = fut.result()
            if err:
                fail += 1
            elif kind == 'kline':
                c_fetched += n
            else:
                f_fetched += n

    # 等待队列全部写完（确保下一轮能看到最新 MAX(ts)）
    queue.join()
    return c_fetched, f_fetched, fail


def sleep_until_next_candle(bar, round_delay):
    cycle = bar_to_seconds(bar)
    now = time.time()
    next_run = (int(now) // cycle + 1) * cycle + round_delay
    sleep_seconds = max(1, next_run - now)
    next_dt = datetime.fromtimestamp(next_run)
    logger.info(f'下一轮开始时间: {next_dt:%Y-%m-%d %H:%M:%S}，等待 {sleep_seconds:.0f}s')
    waited = 0
    while waited < sleep_seconds and not _shutdown:
        time.sleep(1)
        waited += 1


def main():
    args = parse_args()
    cfg = Config()

    setup_logging(name="sync_daemon",
                  level=args.log_level or cfg.logging.level,
                  file_enabled=cfg.logging.file_enabled,
                  max_bytes=cfg.logging.max_bytes,
                  backup_count=cfg.logging.backup_count)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    do_kline = args.kline_only or (not args.funding_only)
    do_funding = args.funding_only or (not args.kline_only)

    client = None
    write_queue = None
    writer_thread = None
    try:
        init_db()
        client = OKXClient()

        write_queue = Queue(maxsize=2000)
        writer_thread = threading.Thread(
            target=writer_loop, args=(write_queue,),
            daemon=True)
        writer_thread.start()

        if args.insts:
            contracts = [{'instId': i.strip(), 'listTime': None}
                         for i in args.insts.split(',') if i.strip()]
        else:
            contracts = get_swap_contracts(client)

        if not contracts:
            logger.error('没有可同步的合约')
            return

        logger.info('=' * 60)
        logger.info(f'OKX 实时同步守护启动 | bar={args.bar} | '
                    f'K线={do_kline} 资金费率={do_funding} '
                    f'| 合约 {len(contracts)} | 并发 {args.workers}')
        logger.info('每收一根K线后同步增量；合约列表每 %ds 重同步以发现新币'
                    % args.contracts_sync_interval)
        logger.info('=' * 60)

        next_contract_sync = 0
        while not _shutdown:
            # 定时重同步合约列表（发现新币）
            if time.time() >= next_contract_sync:
                contracts = get_swap_contracts(client)
                next_contract_sync = time.time() + args.contracts_sync_interval
                logger.info(f'已同步合约列表，当前 {len(contracts)} 个')

            t0 = time.time()
            c_fetched, f_fetched, fail = run_one_round(
                client, contracts, write_queue, args, do_kline, do_funding)
            dt = time.time() - t0
            logger.info(f'本轮完成: 处理任务完成 | '
                        f'抓到K线 {c_fetched} 根 | 资金费率 {f_fetched} 条 | '
                        f'失败 {fail} | 耗时 {dt:.1f}s')

            if _shutdown:
                break
            sleep_until_next_candle(args.bar, args.round_delay)

    except KeyboardInterrupt:
        logger.warning('用户中断')
    except Exception as e:
        logger.exception(f'程序异常: {e}')
        sys.exit(1)
    finally:
        if write_queue is not None and writer_thread is not None:
            write_queue.put(STOP_SIGNAL)
            write_queue.join()
            writer_thread.join(timeout=10)
        if client:
            client.close()
        dispose_engine()
        logger.info('同步守护已退出。')


if __name__ == '__main__':
    main()
