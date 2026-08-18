"""WebSocket 实时采集入口（Phase 4：仅 Trades）

示例：
    python sync_realtime.py                          # 默认全部 USDT 永续
    python sync_realtime.py --insts BTC-USDT-SWAP
    python sync_realtime.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP --duration 60
"""

import argparse
import signal
import sys
import time

from app.db.database import init_db
from app.client.okx_client import OKXClient
from app.realtime.manager import RealtimeManager
from app.utils.logger import get_logger
from app.config.download_scope import load_scope, resolve_instruments, scope_default

logger = get_logger("sync_realtime")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX WebSocket 实时数据采集")
    parser.add_argument(
        "--insts",
        default=None,
        help="产品ID，逗号分隔，如 BTC-USDT-SWAP,ETH-USDT-SWAP；默认全部 USDT 永续",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="运行时长（秒），默认 0 = 无限",
    )
    parser.add_argument(
        "--channels",
        default="trades",
        help="订阅频道，逗号分隔。可选: trades/orderbook/oi/funding/mark/index/kline",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    init_db()
    scope = load_scope()

    # 币种区域：命令行 --insts > 配置文件（默认全部 USDT 永续）
    explicit = ([i.strip() for i in args.insts.split(",") if i.strip()]
                if args.insts else None)
    inst_ids = resolve_instruments(scope, OKXClient(), explicit=explicit)
    if not inst_ids:
        logger.error("未解析到任何合约（检查下载范围配置）")
        return 1

    # 订阅频道默认值：命令行 > 配置文件
    channels_str = args.channels or scope_default(scope, "channels", "trades")
    channels = [c.strip() for c in channels_str.split(",") if c.strip()]
    manager = RealtimeManager(inst_ids=inst_ids, channels=channels)

    shutdown = False

    def _signal_handler(signum, frame):
        nonlocal shutdown
        shutdown = True
        logger.warning("收到退出信号，正在停止...")
        manager.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    manager.start()
    logger.info("WebSocket 实时采集已启动: %s", inst_ids)

    if args.duration > 0:
        time.sleep(args.duration)
    else:
        while not shutdown:
            time.sleep(1)

    manager.stop()
    logger.info("WebSocket 实时采集已停止")
    return 0


if __name__ == "__main__":
    sys.exit(main())
