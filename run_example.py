"""示例脚本 - 展示如何以编程方式使用各模块

该脚本演示：
1. 初始化日志与数据库
2. 下载指定交易对的K线数据
3. 下载资金费率数据
4. 查询已下载的数据

运行方式：
    python run_example.py
"""

from datetime import timedelta
from utils.logger import setup_logging
from utils.time_utils import utc_now
from database import init_db, dispose_engine, session_scope
from models import Candle, FundingRate
from okx_client import OKXClient
from downloader.candles import CandleDownloader
from downloader.funding import FundingRateDownloader


def main():
    # 1. 初始化日志
    setup_logging(level="DEBUG")

    # 2. 初始化数据库
    init_db()

    # 3. 创建客户端
    client = OKXClient()

    # 4. 设置下载的时间范围（最近30天）
    now = utc_now()
    start = now - timedelta(days=30)

    # 5. 下载KW线
    candle_dl = CandleDownloader(client)
    count = candle_dl.download_range(
        inst_id="ETH-USDT-SWAP",
        bar="4H",
        start=start,
        end=now,
    )
    print(f"\n已下载 ETH-USDT-SWAP 4H K线: {count} 根\n")

    # 6. 读取回查数据示例（ORM查询）
    with session_scope() as session:
        latest = (
            session.query(Candle)
            .filter(Candle.inst_id == "ETH-USDT-SWAP")
            .order_by(Candle.ts.desc())
            .limit(5)
            .all()
        )
        print("最近5根K线：")
        for c in latest:
            print(f"  {c.ts}  O={c.o}  C={c.c}  VOL={c.vol}")

    # 7. 下载资金费率（合约专用）
    funding_dl = FundingRateDownloader(client)
    f_count = funding_dl.download_range(
        inst_id="ETH-USDT-SWAP",
        start=start,
        end=now,
    )
    print(f"\n已下载 ETH-USDT-SWAP 资金费率: {f_count} 条\n")

    # 释放数据库连接
    dispose_engine()


if __name__ == "__main__":
    main()
