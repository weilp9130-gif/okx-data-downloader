"""进度通道测试：JSONL 写入/节流/轮转/增量读取/日志字节偏移"""

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from app.utils.progress import (
    ProgressWriter,
    iter_lines,
    read_progress_events,
    remove_progress_file,
    tail_log,
)


class TestProgressWriter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "attempt-1.jsonl"

    def tearDown(self):
        self.tmp.cleanup()

    def test_write_and_read_events(self):
        with ProgressWriter(self.path).open() as w:
            w.stage("download")
            w.progress(percent=50.0, written=500, expected=1000, rate=10.5)
            w.done(written=500)
        events = list(iter_lines(self.path))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]["type"], "stage")
        self.assertEqual(events[0]["stage"], "download")
        self.assertEqual(events[1]["percent"], 50.0)
        self.assertEqual(events[1]["written"], 500)
        self.assertIsNone(events[1].get("eta_sec"))
        self.assertEqual(events[2]["type"], "done")

    def test_progress_throttle(self):
        w = ProgressWriter(self.path, throttle_sec=1.0).open()
        w.progress(percent=1, written=1)
        w.progress(percent=2, written=2)  # 应被节流
        time.sleep(1.05)
        w.progress(percent=3, written=3)  # 应写入
        w.close()
        events = list(iter_lines(self.path))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["written"], 1)
        self.assertEqual(events[1]["written"], 3)

    def test_written_events_not_throttled(self):
        w = ProgressWriter(self.path, throttle_sec=10.0).open()
        w.written(1)
        w.written(2)
        w.close()
        self.assertEqual(len(list(iter_lines(self.path))), 2)

    def test_rotation(self):
        w = ProgressWriter(self.path, throttle_sec=0, max_bytes=200, max_files=2).open()
        for i in range(200):
            w.emit({"type": "progress", "written": i, "x": "y" * 10})
        w.close()
        rotated = Path(f"{self.path}.1.jsonl")
        self.assertTrue(rotated.exists(), "轮转后 .1.jsonl 应存在")

    def test_remove_progress_file_idempotent(self):
        with ProgressWriter(self.path).open() as w:
            w.done(0)
        remove_progress_file(self.path)
        self.assertFalse(self.path.exists())
        remove_progress_file(self.path)  # 幂等

    def test_read_progress_events_incremental(self):
        with ProgressWriter(self.path).open() as w:
            w.emit({"type": "written", "written": 10})
            w.emit({"type": "written", "written": 20})
        events, offset = read_progress_events(self.path)
        self.assertEqual(len(events), 2)
        self.assertEqual(offset, os.path.getsize(self.path))
        # 第二次读取无新事件
        events2, offset2 = read_progress_events(self.path, start_offset=offset)
        self.assertEqual(events2, [])
        self.assertEqual(offset2, offset)
        # 半行不推进 offset
        with open(self.path, "a", encoding="utf-8") as f:
            f.write('{"type": "partial"')
        events3, offset3 = read_progress_events(self.path, start_offset=offset)
        self.assertEqual(events3, [])
        self.assertEqual(offset3, offset)

    def test_tail_log_bytes(self):
        self.path.write_bytes(b"line1\nline2\n")
        r = tail_log(str(self.path), offset=6)
        self.assertEqual(r["content"], "line2\n")
        self.assertEqual(r["offset"], len("line1\nline2\n"))
        # offset 超界钳制
        r2 = tail_log(str(self.path), offset=9999)
        self.assertEqual(r2["size"], len("line1\nline2\n"))
        self.assertEqual(r2["offset"], r2["size"])

    def test_tail_log_missing(self):
        r = tail_log(str(self.path), offset=0)
        self.assertEqual(r["content"], "")
        self.assertEqual(r["size"], 0)


if __name__ == "__main__":
    unittest.main()
