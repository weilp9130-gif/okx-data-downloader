"""假 CLI：写进度 JSONL → --exit 0/1 或 --sleep（供 Worker 端到端测试）

用法：
    python -m tests.fixtures.fake_task --progress-file <path> [--exit 0|1] [--sleep 0.2]
"""

import argparse
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress-file")
    parser.add_argument("--exit", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    if args.progress_file:
        from app.utils.progress import ProgressWriter

        with ProgressWriter(args.progress_file).open() as w:
            w.stage("download")
            n = 0
            deadline = time.monotonic() + max(args.sleep, 0.05)
            while time.monotonic() < deadline:
                n += 10
                w.progress(percent=min(n, 100), written=n, expected=100,
                           rate=50.0, eta_sec=1)
                time.sleep(0.05)
            w.done(written=n)
    return args.exit


if __name__ == "__main__":
    sys.exit(main())
