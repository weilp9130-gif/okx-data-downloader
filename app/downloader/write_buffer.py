"""写库缓冲：把多个下载线程的 K 线数据集中到独立线程写入 DB

背景
----
阶段1并发可达 124 线程，若每个线程都直接向 PostgreSQL 发批量 INSERT，
会瞬间产生大量并发事务，容易把 DB 压崩（连接/内存/WAL）。

方案
----
所有下载线程只把待写入的数据放进 Queue；由独立后台写库线程消费队列，
串行（或有限并行）写入。这样：
- 下载并发保持 124，不降低拉取速度。
- DB 侧写并发被限流，避免崩溃。
- 下载线程通过 wait() 等待写入完成，自然产生背压。

用法
----
    from app.downloader.write_buffer import get_write_buffer
    written = get_write_buffer().put(rows, overwrite=False)
"""

import queue
import threading
import time
from typing import List, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..database import get_engine
from ..models import Candle
from ..utils.logger import get_logger

logger = get_logger(__name__)

_MAX_BUFFERED_ROWS = 5000        # 单次合并写入上限
_FLUSH_TIMEOUT = 0.1             # 等待更多数据的超时（秒）
_PUT_TIMEOUT = 300.0              # 调用方等待写入完成的超时


class _WriteBuffer:
    def __init__(self):
        self._queue: queue.Queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def _ensure_started(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._stop.clear()
                self._thread = threading.Thread(target=self._writer_loop, daemon=True)
                self._thread.start()
                logger.debug("写库缓冲线程已启动")

    def put(self, rows: List[dict], overwrite: bool = False) -> int:
        """提交一批待写入数据，阻塞直到写入完成或超时

        Args:
            rows: Candle 表行字典列表
            overwrite: 是否覆盖已有数据

        Returns:
            int: 实际新增的行数
        """
        self._ensure_started()
        done = threading.Event()
        result: dict = {}
        self._queue.put((rows, overwrite, done, result))
        if not done.wait(timeout=_PUT_TIMEOUT):
            raise RuntimeError("写库缓冲 flush 超时")
        if "exc" in result:
            raise result["exc"]
        return result.get("rowcount", 0)

    def stop(self, timeout: float = 30.0) -> None:
        """优雅停止写库线程，等待队列消费完"""
        if self._thread is None or not self._thread.is_alive():
            return
        self._stop.set()
        self._queue.put(None)
        self._thread.join(timeout=timeout)

    def _writer_loop(self) -> None:
        while not self._stop.is_set():
            try:
                first = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if first is None:
                break

            # 尽量把队列里积的小批次合并成一次写入，减少 DB 事务数
            items = [first]
            total_rows = len(first[0])
            deadline = time.monotonic() + _FLUSH_TIMEOUT
            while total_rows < _MAX_BUFFERED_ROWS:
                wait = max(0.0, deadline - time.monotonic())
                try:
                    extra = self._queue.get(timeout=wait)
                except queue.Empty:
                    break
                if extra is None:
                    self._stop.set()
                    break
                items.append(extra)
                total_rows += len(extra[0])

            self._flush(items)

    def _flush(self, items: List[tuple]) -> None:
        """合并写入一批数据，并通知各个提交者"""
        if not items:
            return

        # 所有项的 overwrite 标志应该一致；若不一致，按首个处理
        overwrite = items[0][1]
        all_rows = []
        for rows, _, _, _ in items:
            all_rows.extend(rows)

        try:
            rowcount = _write_to_db(all_rows, overwrite)
            # 按行数比例把 rowcount 分配给每个提交者
            total = sum(len(r) for r, _, _, _ in items) or 1
            remain = rowcount
            for idx, (rows, _, done, result) in enumerate(items):
                if idx == len(items) - 1:
                    allocated = remain
                else:
                    allocated = int(rowcount * len(rows) / total)
                    remain -= allocated
                result["rowcount"] = allocated
                done.set()
        except Exception as e:
            logger.error(f"写库缓冲写入失败: {e}")
            for _, _, done, result in items:
                result["exc"] = e
                done.set()


def _write_to_db(rows: List[dict], overwrite: bool) -> int:
    """实际执行一次合并写入，优先使用 COPY + 临时表提速"""
    if not rows:
        return 0

    columns = [
        "inst_id", "bar", "ts", "o", "h", "l", "c",
        "vol", "vol_ccy", "vol_ccy_quote", "confirm",
    ]

    # 构造 COPY 输入（tab 分隔）
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(
        buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
    )
    for r in rows:
        writer.writerow([
            r["inst_id"],
            r["bar"],
            r["ts"].isoformat() if r["ts"] else "",
            str(r["o"]),
            str(r["h"]),
            str(r["l"]),
            str(r["c"]),
            str(r["vol"]),
            str(r["vol_ccy"]) if r["vol_ccy"] is not None else "\\N",
            str(r["vol_ccy_quote"]) if r["vol_ccy_quote"] is not None else "\\N",
            str(r["confirm"]) if r["confirm"] is not None else "\\N",
        ])

    engine = get_engine()
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            "CREATE TEMP TABLE _okx_candle_tmp ("
            "    inst_id TEXT, bar TEXT, ts TIMESTAMPTZ, o NUMERIC, h NUMERIC, "
            "    l NUMERIC, c NUMERIC, vol NUMERIC, vol_ccy NUMERIC, "
            "    vol_ccy_quote NUMERIC, confirm TEXT"
            ") ON COMMIT DROP"
        )
        buf.seek(0)
        cur.copy_from(buf, "_okx_candle_tmp", columns=columns, sep="\t")

        upsert_sql = """
            INSERT INTO candles (inst_id, bar, ts, o, h, l, c, vol, vol_ccy, vol_ccy_quote, confirm)
            SELECT inst_id, bar, ts, o, h, l, c, vol, vol_ccy, vol_ccy_quote, confirm
            FROM _okx_candle_tmp
            ON CONFLICT (inst_id, bar, ts)
        """
        if overwrite:
            upsert_sql += """ DO UPDATE SET
                o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c,
                vol = EXCLUDED.vol, vol_ccy = EXCLUDED.vol_ccy,
                vol_ccy_quote = EXCLUDED.vol_ccy_quote, confirm = EXCLUDED.confirm
            """
        else:
            upsert_sql += " DO NOTHING"

        cur.execute(upsert_sql)
        rowcount = cur.rowcount
        raw.commit()
        return rowcount
    finally:
        raw.close()


# 模块级单例
_buffer: Optional[_WriteBuffer] = None
_buffer_lock = threading.Lock()


def get_write_buffer() -> _WriteBuffer:
    global _buffer
    with _buffer_lock:
        if _buffer is None:
            _buffer = _WriteBuffer()
        return _buffer


