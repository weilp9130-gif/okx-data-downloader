"""Index Price 下载模块"""

from ..models import IndexPrice, IndexPriceSyncState
from .periodic_candle import PeriodicCandleDownloader


class IndexPriceDownloader(PeriodicCandleDownloader):
    """指数价格 K线下载器"""

    model = IndexPrice
    sync_state_model = IndexPriceSyncState
    client_method = "get_index_candles"
    label = "IndexPrice"
