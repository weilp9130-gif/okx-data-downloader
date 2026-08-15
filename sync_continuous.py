"""连续同步下载脚本：启动后一直运行，先全量追赶，再低资源实时同步

两个阶段（日志中明确标注当前阶段）：

阶段1【全量同步/追赶】—— 让数据库数据与OKX API对齐
  - 复用 CandleDownloader.download_range（缺失窗口检测 + 逐页回溯 + 批量入库），
    把每个合约缺失的数据一次性补齐到当前时刻（下载量可能很大）。
  - 此阶段不限制系统资源：高并发（--workers 默认代理池下 IP数×2，最高64）、
    满载运行，只为尽快追赶。
  - 全部合约追平（lag < --catchup-lag 分钟）后自动进入阶段2。
  - 可 --skip-catchup 跳过，直接实时同步（适用于数据本已最新的场景）。

阶段2【实时同步】—— 低资源后台无感持续下载
  - 每轮只取增量（默认每合约最多 --max-pages 页），有界队列 + 独立写库线程，
    幂等入库；追平时按K线周期对齐休眠，CPU/内存占用极低。
  - 默认并发 --rt-workers 4，限制资源占用，后台静默运行。
  - 定期重同步合约列表（--contracts-sync-interval）以自动纳入新币。

资源策略：阶段1宽松（快），阶段2受限（省），两阶段都遵守OKX每IP平滑限速
（复用 proxy_pool，避免429风暴——这是API约束而非本机资源限制）。

用法：
    python sync_continuous.py                              # 默认一直运行
    python sync_continuous.py --hours 12                   # 运行12小时后退出
    python sync_continuous.py --skip-catchup               # 跳过追赶，直接实时同步
    python sync_continuous.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
    python sync_continuous.py --dynamic --pool-size 16     # 配合IP代理池提速
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

from app.config import Config
from app.utils.logger import setup_logging, get_logger
from app.database import init_db, dispose_engine, get_engine
from app.okx_client import OKXClient
from app.models import Candle
from app.proxy_pool import build_proxy_pool
from app.downloader.candles import CandleDownloader
from app.utils.time_utils import ms_to_datetime, bar_to_seconds
from app.utils.okx_utils import ms_to_naive_utc, get_swap_contracts

logger = get_logger(__name__)

_shutdown = False
STOP = object()

BANNER = '=' * 60


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning('收到退出信号，冲刷缓冲区后安全退出（已入库数据不丢失）...')


def parse_args():
    p = argparse.ArgumentParser(
        description='OKX 连续同步下载：先全量追赶OKX数据，再低资源实时同步（默认一直运行）')
    p.add_argument('--insts', default=None,
                   help='指定交易对，逗号分隔（默认全部USDT永续）')
    p.add_argument('--bar', default='1m', help='K线粒度，默认1m')
    p.add_argument('--hours', type=float, default=0,
                   help='运行时长(小时)，默认0=无限运行')
    p.add_argument('--workers', type=int, default=None,
                   help='阶段1(全量同步)并发数（代理池下默认IP数×2，最高64；直连默认16）')
    p.add_argument('--rt-workers', type=int, default=4,
                   help='阶段2(实时同步)并发数，默认4（低资源后台运行）')
    p.add_argument('--catchup-lag', type=float, default=10,
                   help='阶段1判定"已追平"的最大落后分钟数，默认10')
    p.add_argument('--catchup-attempts', type=int, default=3,
                   help='阶段1对仍落后的合约最多重试轮数，默认3')
    p.add_argument('--skip-catchup', action='store_true',
                   help='跳过阶段1全量同步，直接进入阶段2实时同步')
    p.add_argument('--contracts-sync-interval', type=int, default=1800,
                   help='阶段2合约列表重同步间隔(秒)，默认1800(30分钟)自动发现新币')
    p.add_argument('--max-pages', type=int, default=10,
                   help='阶段2每轮每合约最多翻几页(每页100根)，默认10')
    p.add_argument('--round-delay', type=int, default=10,
                   help='K线收盘后额外等待秒数，默认10（避免拉到未完结K线）')
    p.add_argument('--queue-size', type=int, default=5000,
                   help='阶段2写库队列容量上限(有界，控制内存)，默认5000')
    p.add_argument('--db-batch', type=int, default=1000,
                   help='阶段2写库批量条数，默认1000')
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
# 合约列表 / 水位线
# ----------------------------------------------------------------------
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


def refresh_watermark(inst, bar):
    """把某合约在库内的 MAX(ts) 回写 sync_state（阶段1用，逐合约走索引）"""
    engine = get_engine()
    with engine.connect() as conn:
        m = conn.execute(
            text("SELECT MAX(ts) FROM candles WHERE inst_id = :i AND bar = :b"),
            {"i": inst, "b": bar}).scalar()
        if m is not None:
            conn.execute(text(
                "INSERT INTO sync_state (inst_id, bar, latest_ts, updated_at) "
                "VALUES (:i, :b, :t, now()) "
                "ON CONFLICT (inst_id, bar) DO UPDATE SET "
                "latest_ts = GREATEST(sync_state.latest_ts, EXCLUDED.latest_ts), "
                "updated_at = now()"),
                {"i": inst, "b": bar, "t": m})
        conn.commit()


def batch_refresh_watermark(inst_ids, bar):
    """批量刷新一组合约的水位线（减少短连接）"""
    if not inst_ids:
        return
    engine = get_engine()
    with engine.connect() as conn:
        for inst in inst_ids:
            m = conn.execute(
                text("SELECT MAX(ts) FROM candles WHERE inst_id = :i AND bar = :b"),
                {"i": inst, "b": bar}).scalar()
            if m is not None:
                conn.execute(text(
                    "INSERT INTO sync_state (inst_id, bar, latest_ts, updated_at) "
                    "VALUES (:i, :b, :t, now()) "
                    "ON CONFLICT (inst_id, bar) DO UPDATE SET "
                    "latest_ts = GREATEST(sync_state.latest_ts, EXCLUDED.latest_ts), "
                    "updated_at = now()"),
                    {"i": inst, "b": bar, "t": m})
        conn.commit()


def catchup_lag_minutes(bar, contracts):
    """所有已同步合约中最落后的分钟数（无水位的合约不参与判定）"""
    latest = get_latest_ts_map(bar)
    now_dt = datetime.now(timezone.utc)
    lags = []
    for c in contracts:
        lt = latest.get(c['instId'])
        if lt is not None:
            lags.append((now_dt - lt).total_seconds() / 60.0)
    return max(lags) if lags else 0.0


# ----------------------------------------------------------------------
# 阶段1：全量同步（追赶，不限制系统资源）
# ----------------------------------------------------------------------
def _catchup_contract(candle_dl, inst, bar, end_dt, list_time_ms):
    try:
        # listTime 来自 OKX 接口为字符串，需转 int（ms_to_datetime 内部有 / 1000）
        start = ms_to_naive_utc(int(list_time_ms)) if list_time_ms else None
        n = candle_dl.download_range(
            inst, bar, start=start, end=end_dt, list_time=start)
        return inst, n, None
    except Exception as e:
        return inst, 0, str(e)


def catch_up_pass(candle_dl, contracts, bar, end_dt, workers):
    """一轮全量追赶：所有合约并行补齐到 end_dt，返回成功合约列表"""
    done = ok = fail = 0
    total = 0
    t0 = time.time()
    last_log = time.time()
    success_insts = []

    def work(c):
        inst, n, err = _catchup_contract(
            candle_dl, c['instId'], bar, end_dt, c.get('listTime'))
        return inst, n, err

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, c): c['instId'] for c in contracts}
        for fut in as_completed(futs):
            inst, n, err = fut.result()
            done += 1
            if err:
                fail += 1
                logger.error(f'[阶段1 全量同步] {inst}: {err}')
            else:
                ok += 1
                total += n
                success_insts.append(inst)
            now = time.time()
            if (done % 20 == 0 or done == len(contracts)
                    or now - last_log >= 30):
                rate = total / max(now - t0, 0.1)
                logger.info(
                    f'[阶段1 全量同步] 进度 {done}/{len(contracts)} 合约 '
                    f'(成功{ok} 失败{fail}) | 累计写入 {total:,} 根 | '
                    f'速率 {rate:,.0f} 根/秒 | 已用 {(now - t0) / 60:.1f} 分')
                last_log = now
    # 本轮所有成功合约统一批量刷新水位线，避免逐合约短连接
    batch_refresh_watermark(success_insts, bar)
    return total, fail


def run_phase1(candle_dl, contracts, bar, args):
    """阶段1主循环：追赶直到全部追平或重试轮数用尽"""
    logger.info(BANNER)
    logger.info('[阶段1 全量同步] 开始：将数据库数据与OKX API对齐到当前时刻')
    logger.info(f'[阶段1 全量同步] 合约 {len(contracts)} 个 | 并发 {args.workers} '
                f'| 本阶段不限制系统资源，满载追赶')
    logger.info(BANNER)

    end_dt = datetime.now(timezone.utc).replace(tzinfo=None)
    attempts = 0
    total_written = 0
    total_fail = 0
    while not _shutdown and attempts < args.catchup_attempts:
        attempts += 1
        logger.info(f'[阶段1 全量同步] 第 {attempts}/{args.catchup_attempts} 轮追赶开始...')
        written, fail = catch_up_pass(candle_dl, contracts, bar, end_dt, args.workers)
        total_written += written
        total_fail += fail
        lag = catchup_lag_minutes(bar, contracts)
        logger.info(
            f'[阶段1 全量同步] 第 {attempts} 轮完成：写入 {written:,} 根 | '
            f'失败 {fail} | 当前最大落后 {lag:.1f} 分钟')
        if lag <= args.catchup_lag:
            break
        # 仍有落后：对失败/落后的合约再补一轮
        if attempts < args.catchup_attempts and not _shutdown:
            logger.info(f'[阶段1 全量同步] 仍有 {lag:.1f} 分钟落后，重试补齐...')

    lag = catchup_lag_minutes(bar, contracts)
    logger.info(BANNER)
    if lag <= args.catchup_lag:
        logger.info(f'[阶段1 全量同步] 完成 | 最大落后 {lag:.1f} 分钟 -> 进入阶段2 实时同步')
    elif total_fail >= len(contracts) and total_written == 0:
        # 全部合约异常且未写入任何数据：属系统性故障，如实提示而非宣称"完成"
        logger.error(
            f'[阶段1 全量同步] 追赶失败：{total_fail} 个合约全部异常、未写入任何数据，'
            f'当前最大落后 {lag:.1f} 分钟。请检查网络/代理/日志后重新启动，'
            f'本次进入阶段2 仅作增量同步，无法完成历史补齐。')
    else:
        logger.warning(
            f'[阶段1 全量同步] 未完全追平（失败 {total_fail} 个合约，'
            f'最大落后 {lag:.1f} 分钟），先进入阶段2 实时同步继续增量追赶')
    logger.info(BANNER)


# ----------------------------------------------------------------------
# 阶段2：实时同步（增量拉取 + 批量写库，低资源）
# ----------------------------------------------------------------------
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
            logger.info(f'[阶段2 实时同步] 写库线程退出: 累计入库 '
                        f'{stats["rows"]} 根 | 刷库 {stats["flushes"]} 次')
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
                logger.error(f'[阶段2 实时同步] {inst}: {err}')
            elif rows:
                queue.put(('candles', rows))
                fetched += len(rows)
    return fetched, errors


def sleep_interruptible(seconds):
    end = time.time() + seconds
    while time.time() < end and not _shutdown:
        time.sleep(min(1.0, end - time.time()))


def run_phase2(client, contracts, bar, args):
    """阶段2主循环：低资源实时同步，直到 --hours 到期或收到退出信号"""
    queue = Queue(maxsize=args.queue_size)
    idle_event = threading.Event()
    writer = threading.Thread(
        target=writer_loop,
        args=(queue, args.db_batch, 2.0, idle_event, bar),
        daemon=True)
    writer.start()

    logger.info(BANNER)
    logger.info(f'[阶段2 实时同步] 开始：低资源后台增量同步 '
                f'| 并发 {args.rt_workers} | 每合约最多 {args.max_pages} 页/轮')
    logger.info(BANNER)

    t_start = time.time()
    total_rows = 0
    rounds = 0
    errors_total = 0
    cycle = bar_to_seconds(bar)
    stop_time = t_start + args.hours * 3600 if args.hours > 0 else None
    next_contract_sync = 0
    contracts_list = list(contracts)

    while not _shutdown:
        if stop_time is not None and time.time() >= stop_time:
            logger.info(f'[阶段2 实时同步] 运行时长已达 {args.hours:g} 小时，正常结束')
            break

        # 定时重同步合约列表，自动纳入新币
        if time.time() >= next_contract_sync:
            try:
                contracts_list = get_swap_contracts(client) \
                    if not args.insts else contracts_list
                next_contract_sync = time.time() + args.contracts_sync_interval
                logger.info(f'[阶段2 实时同步] 已重同步合约列表，'
                            f'当前 {len(contracts_list)} 个')
            except Exception as e:
                logger.warning(f'[阶段2 实时同步] 合约列表重同步失败: {e}')

        t_round = time.time()
        latest_map = get_latest_ts_map(bar)
        fetched, errors = sync_round(
            client, contracts_list, latest_map, queue, args.rt_workers,
            bar, args.max_pages)
        # 等待写库线程把本轮数据全部落库，保证下一轮能看到最新水位
        idle_event.clear()
        queue.join()
        idle_event.wait(timeout=5.0)
        rounds += 1
        errors_total += errors
        total_rows += fetched
        dt_round = time.time() - t_round

        logger.info(
            f'[阶段2 实时同步] 第{rounds}轮 | 抓到 {fetched} 根 | 失败 {errors} | '
            f'累计 {total_rows:,} 根 | 本轮耗时 {dt_round:.1f}s | '
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

    # 冲刷剩余数据并退出写库线程
    queue.put(STOP)
    queue.join()
    writer.join(timeout=15)
    logger.info('-' * 60)
    logger.info(f'[阶段2 实时同步] 结束: 运行 {rounds} 轮 | 共抓取 {total_rows:,} 根 '
                f'| 失败 {errors_total} 次')


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def main():
    args = parse_args()
    cfg = Config()

    setup_logging(name="sync_continuous",
                  level=args.log_level or cfg.logging.level,
                  file_enabled=cfg.logging.file_enabled,
                  max_bytes=cfg.logging.max_bytes,
                  backup_count=cfg.logging.backup_count)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    client = None
    try:
        init_db()
        pool = build_proxy_pool(args)
        client = OKXClient(proxy_pool=pool)
        candle_dl = CandleDownloader(client, cfg)

        # 阶段1并发默认值：代理池下每IP 4个并发(上限96)，写库已改每批短连接不受DB池限制
        if args.workers is None:
            args.workers = max(4, min(4 * len(pool), 96)) if pool else 16

        contracts = ([{'instId': i.strip()} for i in args.insts.split(',') if i.strip()]
                     if args.insts else get_swap_contracts(client))
        if not contracts:
            logger.error('没有可同步的合约')
            return

        # 初始化水位线（逐合约MAX(ts)，避免全表扫描）
        ensure_sync_state(args.bar, [c['instId'] for c in contracts])

        logger.info(BANNER)
        logger.info('OKX 连续同步下载启动（先追赶后实时，两阶段）')
        logger.info(f'K线粒度 {args.bar} | 合约 {len(contracts)} | '
                    f'阶段1并发 {args.workers} | 阶段2并发 {args.rt_workers}')
        if pool is not None:
            logger.info(f'代理池模式: {pool.stats()}')
        logger.info(BANNER)

        # ===== 阶段1：全量同步（追赶），不限制系统资源 =====
        if args.skip_catchup:
            logger.info('[阶段1 全量同步] 已通过 --skip-catchup 跳过，直接进入阶段2')
        else:
            run_phase1(candle_dl, contracts, args.bar, args)

        # ===== 阶段2：实时同步（低资源后台） =====
        if _shutdown:
            logger.info('收到退出信号，跳过阶段2')
        else:
            run_phase2(client, contracts, args.bar, args)

    except KeyboardInterrupt:
        logger.warning('用户中断')
    except Exception as e:
        logger.exception(f'程序异常: {e}')
        sys.exit(1)
    finally:
        if client:
            client.close()
        dispose_engine()
        logger.info('连续同步下载已退出。')


if __name__ == '__main__':
    main()
