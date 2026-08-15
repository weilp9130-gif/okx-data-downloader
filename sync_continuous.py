"""连续同步下载脚本：启动后持续下载最新K线数据，默认24小时后自动退出

目标：内存/CPU占用低 + 下载速度快，并自动平衡二者
- 内存低：有界队列(Queue maxsize) + 独立批量写库线程，每轮只取增量(最多
  max-pages页)，不缓存全量数据；写入采用 ON CONFLICT DO NOTHING 幂等入库
- CPU低：追平时按K线周期对齐休眠（长睡省CPU）；落后时呼吸式短休连续拉取；
  会话复用(keep-alive)避免重复TLS握手；按轮汇总日志，不逐请求打日志
- 速度快：多合约并行(ThreadPoolExecutor) + 可选IP代理池每IP平滑限速
  （复用 proxy_pool 的平滑限速器，实测8IP可达~53 pages/s且几乎无429）

同步逻辑：
- 每轮先查库内每合约最新时间(单次GROUP BY)，再从最新处往前翻最多 max-pages 页，
  只保留已完结(confirm=='1')且新于库内的K线；攒够批量即由写库线程入库。
- 追平(本轮取到0行或耗时短于周期) -> 睡到下一根K线收盘后再同步；
  落后(仍在补缺口) -> 呼吸1秒继续，以接近满载的速度追赶。
- 到期(--hours，默认24)或收到 Ctrl+C/SIGTERM 后，冲刷队列安全退出。

用法：
    python sync_continuous.py                             # 默认24小时, 1m K线
    python sync_continuous.py --hours 0                   # 无限运行
    python sync_continuous.py --bar 5m --hours 12         # 自定义粒度/时长
    python sync_continuous.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
    python sync_continuous.py --dynamic --pool-size 16    # 配合IP代理池提速
"""

import argparse
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from queue import Queue, Empty

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import Config
from utils.logger import setup_logging, get_logger
from database import init_db, dispose_engine, get_engine
from okx_client import OKXClient
from models import Candle
from proxy_pool import build_proxy_pool
from utils.time_utils import ms_to_datetime, utc_now, bar_to_seconds

logger = get_logger(__name__)

_shutdown = False
STOP = object()


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到退出信号，冲刷缓冲区后安全退出（已入库数据不丢失）...')


def parse_args():
    p = argparse.ArgumentParser(
        description='OKX 连续同步下载（默认24小时后自动退出，低内存/低CPU）')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--hours', type=float, default=24,
                   help='运行时长(小时)，默认24；0=无限运行')
    p.add_argument('--workers', type=int, default=None,
                   help='并行下载数（代理池下默认IP数×2，直连默认8）')
    p.add_argument('--max-pages', type=int, default=10,
                   help='每轮每合约最多翻几页(每页100根)，默认10')
    p.add_argument('--round-delay', type=int, default=10,
                   help='K线收盘后额外等待秒数，默认10（避免拉到未完结K线）')
    p.add_argument('--queue-size', type=int, default=5000,
                   help='写库队列容量上限(有界，控制内存)，默认5000')
    p.add_argument('--db-batch', type=int, default=1000,
                   help='写库批量条数，默认1000')
    p.add_argument('--log-level',
                   choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                   default=None, help='日志级别')
    # 代理池参数（与 download_all 保持一致）
    p.add_argument('--proxy-pool', action='store_true',
                   help='启用IP代理池（需配置OKX_PROXY_URLS，每币一个IP）')
    p.add_argument('--proxy-verify', action='store_true',
                   help='启动时探测代理池各代理出口IP并统计独立IP数')
    p.add_argument('--per-ip-rate', type=int, default=None,
                   help='每个IP的请求限速（默认读配置OKX_IP_RATE_LIMIT_PER_SECOND）')
    p.add_argument('--dynamic', action='store_true',
                   help='动态IP池：每次下载前自动发现节点/测IP/应用listeners')
    p.add_argument('--pool-size', type=int, default=16,
                   help='动态IP池的独立IP数量上限，默认16')
    p.add_argument('--pool-ttl', type=int, default=0,
                   help='复用节点IP缓存秒数，0=每次重测（默认）')
    p.add_argument('--pool-base-port', type=int, default=7891,
                   help='动态IP池监听起始端口，默认7891')
    p.add_argument('--no-prompt', action='store_true',
                   help='动态模式等待端口就绪超时后不交互提示，直接报错')
    return p.parse_args()


# ----------------------------------------------------------------------
# 合约列表 / 增量拉取
# ----------------------------------------------------------------------
def get_swap_contracts(client):
    data = client.get_instruments('SWAP')
    contracts = []
    for d in data:
        if d.get('settleCcy') == 'USDT' and d.get('state') == 'live':
            contracts.append({'instId': d['instId']})
    contracts.sort(key=lambda x: x['instId'])
    return contracts


def ensure_sync_state(bar, inst_ids):
    """初始化 sync_state 水位线表（每合约最新K线时间戳）

    为缺少水位线的合约补一次 MAX(ts)。注意不能写 "SELECT inst_id, MAX(ts)
    FROM candles WHERE bar=:b GROUP BY inst_id" 这样的全表聚合——candles 表
    极大(亿级行)，会全表扫描耗时数分钟；逐合约查询走 (inst_id, bar, ts) 主键
    反向索引，O(1) 恒定快速。之后水位线由写库线程随写随更。
    """
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS sync_state ("
            "inst_id VARCHAR(50) NOT NULL,"
            "bar VARCHAR(10) NOT NULL,"
            "latest_ts TIMESTAMPTZ,"
            "updated_at TIMESTAMPTZ DEFAULT now(),"
            "PRIMARY KEY (inst_id, bar))"))
        existing = {r[0] for r in conn.execute(
            text("SELECT inst_id FROM sync_state WHERE bar = :b"),
            {"b": bar}).all()}
        for inst in inst_ids:
            if inst in existing:
                continue
            m = conn.execute(
                text("SELECT MAX(ts) FROM candles "
                     "WHERE inst_id = :i AND bar = :b"),
                {"i": inst, "b": bar}).scalar()
            if m is not None:
                conn.execute(text(
                    "INSERT INTO sync_state (inst_id, bar, latest_ts, updated_at) "
                    "VALUES (:i, :b, :t, now()) ON CONFLICT DO NOTHING"),
                    {"i": inst, "b": bar, "t": m})
        conn.commit()


def get_latest_ts_map(bar):
    """读取 sync_state 水位线: {inst_id: latest_ts}（小表，恒定快速）"""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT inst_id, latest_ts FROM sync_state WHERE bar = :b"),
            {"b": bar}).mappings().all()
    return {r["inst_id"]: r["latest_ts"] for r in rows if r["latest_ts"] is not None}


def fetch_tail(client, inst_id, bar, latest_dt, max_pages):
    """从最新往前翻页，只保留已完结且新于库内 latest_dt 的K线（内存有界）"""
    rows = []
    after = None
    for _ in range(max_pages):
        data = client.get_candles(inst_id=inst_id, bar=bar, after=after, limit=100)
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
                    'ts': ts_dt, 'o': c['o'], 'h': c['h'], 'l': c['l'],
                    'c': c['c'], 'vol': c['vol'], 'vol_ccy': c['vol_ccy'],
                    'vol_ccy_quote': c['vol_ccy_quote'], 'confirm': c['confirm'],
                })
        after = data[-1]['ts']
        if hit_old:
            break
    if not rows:
        return []
    dedup = {}
    for r in rows:
        dedup[r['ts']] = r
    return [dedup[k] for k in sorted(dedup.keys())]


def fetch_worker(client, contract, latest_map, bar, max_pages):
    inst = contract['instId']
    latest = latest_map.get(inst)
    try:
        rows = fetch_tail(client, inst, bar, latest, max_pages)
        return inst, rows, None
    except Exception as e:
        return inst, [], str(e)


# ----------------------------------------------------------------------
# 写库
# ----------------------------------------------------------------------
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


def upsert_sync_state(inst_max):
    """把写库线程维护的水位线(inst_id, bar) -> 最新ts 写回 sync_state 表"""
    if not inst_max:
        return
    engine = get_engine()
    rows = [{"i": inst, "b": bar, "t": ts}
            for (inst, bar), ts in inst_max.items()]
    with engine.connect() as conn:
        conn.execute(text(
            "INSERT INTO sync_state (inst_id, bar, latest_ts, updated_at) "
            "VALUES (:i, :b, :t, now()) "
            "ON CONFLICT (inst_id, bar) DO UPDATE SET "
            "latest_ts = GREATEST(sync_state.latest_ts, EXCLUDED.latest_ts), "
            "updated_at = now()"), rows)
        conn.commit()


def writer_loop(queue, batch_size, flush_interval=2.0, idle_event=None, bar='1m'):
    """独立写库线程：攒够批量或超时即刷库，内存有界

    队列清空且批次刷完后置位 idle_event，供主循环确认"库已最新"再开始下一轮，
    避免下一轮的增量查询读不到本轮刚写的数据而重复拉取。
    同时维护 sync_state 水位线(inst_id, bar) -> 最新ts，随写随更。
    """
    batch = []
    inst_max = {}
    last_flush = time.time()
    stats = {'rows': 0, 'flushes': 0}
    while True:
        item = None
        try:
            item = queue.get(timeout=flush_interval)
        except Empty:
            pass
        now = time.time()
        if item is STOP:
            if batch:
                stats['rows'] += insert_candles_batch(batch)
                upsert_sync_state(inst_max)
                batch.clear()
                inst_max.clear()
                stats['flushes'] += 1
            if idle_event is not None:
                idle_event.set()
            logger.info(f'写库线程退出: 累计入库 {stats["rows"]} 根 | '
                        f'刷库 {stats["flushes"]} 次')
            queue.task_done()
            return
        if item is not None:
            _, rows = item
            batch.extend(rows)
            for r in rows:
                key = (r['inst_id'], bar)
                if key not in inst_max or r['ts'] > inst_max[key]:
                    inst_max[key] = r['ts']
            queue.task_done()
        if batch and (len(batch) >= batch_size or now - last_flush >= flush_interval):
            stats['rows'] += insert_candles_batch(batch)
            upsert_sync_state(inst_max)
            last_flush = now
            batch.clear()
            inst_max.clear()
            stats['flushes'] += 1
        if idle_event is not None and queue.empty() and not batch:
            idle_event.set()


# ----------------------------------------------------------------------
# 单轮同步
# ----------------------------------------------------------------------
def sync_round(client, contracts, latest_map, queue, workers, bar, max_pages):
    fetched = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_worker, client, c, latest_map, bar, max_pages)
                for c in contracts}
        for fut in as_completed(futs):
            if _shutdown:
                break
            inst, rows, err = fut.result()
            if err:
                errors += 1
                logger.error(f'{inst}: {err}')
            elif rows:
                queue.put(('candles', rows))
                fetched += len(rows)
    return fetched, errors


def sleep_interruptible(seconds):
    end = time.time() + seconds
    while time.time() < end and not _shutdown:
        time.sleep(min(1.0, end - time.time()))


# ----------------------------------------------------------------------
# 主循环
# ----------------------------------------------------------------------
def main():
    args = parse_args()
    cfg = Config()

    setup_logging(level=args.log_level or cfg.logging.level,
                  file_enabled=cfg.logging.file_enabled,
                  max_bytes=cfg.logging.max_bytes,
                  backup_count=cfg.logging.backup_count)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    client = None
    queue = None
    writer = None
    try:
        init_db()
        pool = build_proxy_pool(args)
        client = OKXClient(proxy_pool=pool)

        # 并发默认值：代理池下每IP 2个并发，直连8个
        if args.workers is None:
            args.workers = max(2, min(2 * len(pool), 32)) if pool else 8

        contracts = ([{'instId': i.strip()} for i in args.insts.split(',') if i.strip()]
                     if args.insts else get_swap_contracts(client))
        if not contracts:
            logger.error('没有可同步的合约')
            return
        # 初始化水位线（逐合约MAX(ts)，避免全表扫描）
        ensure_sync_state(args.bar, [c['instId'] for c in contracts])

        queue = Queue(maxsize=args.queue_size)
        idle_event = threading.Event()
        writer = threading.Thread(
            target=writer_loop,
            args=(queue, args.db_batch, 2.0, idle_event, args.bar),
            daemon=True)
        writer.start()

        hours_desc = f'{args.hours:g}小时' if args.hours > 0 else '无限'
        t_start = time.time()
        logger.info('=' * 60)
        logger.info(f'连续同步下载启动 | {args.bar} | 时长 {hours_desc} | '
                    f'合约 {len(contracts)} | 并发 {args.workers}')
        if pool is not None:
            logger.info(f'代理池模式: {pool.stats()}')
        logger.info(f'每轮每合约最多 {args.max_pages} 页 | 写库批量 {args.db_batch}')
        logger.info('=' * 60)

        total_rows = 0
        rounds = 0
        errors_total = 0
        cycle = bar_to_seconds(args.bar)
        stop_time = t_start + args.hours * 3600 if args.hours > 0 else None

        while not _shutdown:
            if stop_time is not None and time.time() >= stop_time:
                logger.info(f'运行时长已达 {args.hours:g} 小时，正常结束')
                break

            t_round = time.time()
            latest_map = get_latest_ts_map(args.bar)
            fetched, errors = sync_round(
                client, contracts, latest_map, queue, args.workers,
                args.bar, args.max_pages)
            # 等待写库线程把本轮数据全部落库，保证下一轮能看到最新水位
            # （join 只等消费完成，此处再等批次落库，避免下一轮重复拉取）
            idle_event.clear()
            queue.join()
            idle_event.wait(timeout=5.0)
            rounds += 1
            errors_total += errors
            total_rows += fetched
            dt_round = time.time() - t_round

            logger.info(
                f'[第{rounds}轮] 抓到 {fetched} 根 | 失败 {errors} | '
                f'累计 {total_rows} 根 | 本轮耗时 {dt_round:.1f}s | '
                f'已运行 {(time.time() - t_start) / 3600:.2f}h')

            if _shutdown:
                break

            # 调度：追平则睡到下一根K线收盘后再同步（省CPU）；
            # 落后(取到数据且本轮接近/超过一个周期)则呼吸1秒继续拉取（保速度）
            if fetched == 0 or dt_round < cycle * 0.8:
                next_run = (int(time.time()) // cycle + 1) * cycle + args.round_delay
                sleep_until = max(0.5, next_run - time.time())
            else:
                sleep_until = 1.0
            # 不超过预算截止点，到点立即退出
            if stop_time is not None:
                sleep_until = max(0.0, min(sleep_until, stop_time - time.time()))
            sleep_interruptible(sleep_until)

        logger.info('-' * 60)
        logger.info(f'同步结束: 运行 {rounds} 轮 | 共抓取 {total_rows} 根 | '
                    f'失败 {errors_total} 次')

    except KeyboardInterrupt:
        logger.warning('用户中断')
    except Exception as e:
        logger.exception(f'程序异常: {e}')
        sys.exit(1)
    finally:
        # 冲刷剩余数据并退出写库线程
        if queue is not None and writer is not None:
            try:
                queue.put(STOP)
                queue.join()
                writer.join(timeout=15)
            except Exception:
                pass
        if client:
            client.close()
        dispose_engine()
        logger.info('连续同步下载已退出。')


if __name__ == '__main__':
    main()
