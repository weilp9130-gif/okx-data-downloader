"""任务进度 JSONL 通道（CLI → Worker）

CLI 子进程把进度事件以 JSONL 写入 `--progress-file`，Worker 增量消费写 DB
`jobs.progress`。事件格式：
    {"type": "stage", "stage": "download"}
    {"type": "written", "written": 100, "expected": 1000}
    {"type": "progress", "percent": 12.3, "written": 123, "expected": 1000,
     "rate": 45.6, "eta_sec": 78}
    {"type": "done", "written": 1000}
    {"type": "error", "message": "..."}
"""

import json
import time
from pathlib import Path
from typing import Dict, Iterator, Optional


class ProgressWriter:
    """JSONL 进度文件写入器（含节流与 10MB 轮转）

    - 写事件前记录时间，距离上次写 progress 事件 < throttle_sec 时跳过
      （written/rate 等低频事件仍直接写入）。
    - 文件超过 max_bytes 时轮转为 `{path}.1.jsonl`，保留 max_files 份。
    - 线程安全（子进程通常单线程，此处仍加锁兜底）。
    """

    ROTATE_SUFFIX = ".1.jsonl"
    MAX_FILES = 3

    def __init__(
        self,
        path,
        throttle_sec: float = 0.5,
        max_bytes: int = 10 * 1024 * 1024,
        max_files: int = 3,
    ):
        self.path = Path(path)
        self.throttle_sec = throttle_sec
        self.max_bytes = max_bytes
        self.max_files = max_files
        self._fh = None
        self._last_write = 0.0
        self._lock = False

    def open(self) -> "ProgressWriter":
        if self._fh is not None:
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        return self

    def close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass
            self._fh = None

    def __enter__(self) -> "ProgressWriter":
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    def _throttled(self) -> bool:
        now = time.monotonic()
        if now - self._last_write < self.throttle_sec:
            return True
        self._last_write = now
        return False

    def _rotate_if_needed(self) -> None:
        if self._fh is None:
            return
        try:
            if self._fh.tell() < self.max_bytes:
                return
        except OSError:
            return
        self._fh.flush()
        self._fh.close()
        self._fh = None

        # 轮转：{path} → {path}.1.jsonl → {path}.2.jsonl → ... → 删除最旧
        for i in range(self.max_files - 1, 0, -1):
            src = Path(f"{self.path}.{i}.jsonl")
            dst = Path(f"{self.path}.{i + 1}.jsonl")
            if src.exists():
                try:
                    if dst.exists():
                        dst.unlink()
                    src.rename(dst)
                except OSError:
                    pass
        if self.path.exists():
            try:
                Path(f"{self.path}.1.jsonl").unlink(missing_ok=True)
                self.path.rename(f"{self.path}.1.jsonl")
            except OSError:
                pass

        self._fh = open(self.path, "a", encoding="utf-8")

    def emit(self, event: Dict) -> None:
        if self._fh is None:
            self.open()
        if event.get("type") == "progress" and self._throttled():
            return
        self._rotate_if_needed()
        line = json.dumps(event, ensure_ascii=False, default=str)
        try:
            self._fh.write(line + "\n")
            self._fh.flush()
        except OSError:
            pass

    # ---- 便捷方法 ----------------------------------------------------
    def stage(self, stage: str) -> None:
        self.emit({"type": "stage", "stage": stage})

    def progress(
        self,
        percent=None,
        written=None,
        expected=None,
        rate=None,
        eta_sec=None,
    ) -> None:
        event = {"type": "progress"}
        if percent is not None:
            event["percent"] = round(float(percent), 1)
        if written is not None:
            event["written"] = int(written)
        if expected is not None:
            event["expected"] = int(expected)
        if rate is not None:
            event["rate"] = round(float(rate), 2)
        if eta_sec is not None:
            event["eta_sec"] = max(0, int(eta_sec))
        self.emit(event)

    def written(self, written: int, expected: Optional[int] = None) -> None:
        event = {"type": "written", "written": int(written)}
        if expected is not None:
            event["expected"] = int(expected)
        self.emit(event)

    def done(self, written: int = 0) -> None:
        self.emit({"type": "done", "written": int(written)})

    def error(self, message: str) -> None:
        self.emit({"type": "error", "message": str(message)})


def iter_lines(path, start_offset: int = 0, encoding: str = "utf-8") -> Iterator[Dict]:
    """增量读取 JSONL 文件，产出已解析的事件 dict（跳过空行/坏行）"""
    f = open(path, "r", encoding=encoding)
    try:
        f.seek(start_offset)
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except (ValueError, TypeError):
                continue
            if isinstance(event, dict):
                yield event
    finally:
        f.close()


def read_progress_events(
    path, start_offset: int = 0, limit_bytes: int = 2 * 1024 * 1024,
    encoding: str = "utf-8",
) -> tuple:
    """增量读取 JSONL，返回 (events, new_offset)

    仅推进到最后一个完整行（以 \\n 结尾）的字节偏移，避免读到子进程
    正在写入的半行。
    """
    p = Path(path)
    if not p.exists():
        return [], start_offset
    size = p.stat().st_size
    offset = min(max(0, start_offset), size)
    with open(p, "rb") as f:
        f.seek(offset)
        data = f.read(limit_bytes)
    if not data:
        return [], offset
    # 只保留最后一个完整行
    newline_idx = data.rfind(b"\n")
    if newline_idx == -1:
        return [], offset
    full = data[:newline_idx + 1]
    new_offset = offset + len(full)
    text = full.decode(encoding, errors="replace")
    events = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(event, dict):
            events.append(event)
    return events, new_offset


def remove_progress_file(path) -> None:
    """终态删除 progress 文件（幂等；轮转备份一并删除）"""
    p = Path(path)
    for suffix in ("", *[".%d" % i for i in range(1, 5)]):
        try:
            (Path(f"{p}{suffix}.jsonl") if suffix else p).unlink(missing_ok=True)
        except OSError:
            pass


def tail_log(path, offset: int = 0, limit_bytes: int = 256 * 1024, encoding: str = "utf-8") -> dict:
    """按字节偏移增量读取日志文件

    Returns:
        {"content": str, "offset": int(新偏移), "size": int(文件当前大小)}
    """
    p = Path(path)
    if not p.exists():
        return {"content": "", "offset": offset, "size": 0}
    size = p.stat().st_size
    offset = min(max(0, offset), size)
    with open(p, "rb") as f:
        f.seek(offset)
        data = f.read(limit_bytes)
    try:
        content = data.decode(encoding, errors="replace")
    except UnicodeDecodeError:
        content = ""
    return {"content": content, "offset": offset + len(data), "size": size}
