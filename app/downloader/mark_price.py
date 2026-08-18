"""Mark Price 下载模块"""

from ..db.models import MarkPrice, MarkPriceSyncState
from .periodic_candle import PeriodicCandleDownloader


class MarkPriceDownloader(PeriodicCandleDownloader):
    """标记价格 K线下载器"""

    model = MarkPrice
    sync_state_model = MarkPriceSyncState
    client_method = "get_mark_price_candles"
    label = "MarkPrice"
