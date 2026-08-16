"""OrderBook 因子计算"""

from decimal import Decimal
from typing import Dict, List, Optional


class OrderBookFactorCalculator:
    """基于 OrderBook 快照计算因子"""

    @staticmethod
    def calculate(bids: List[List[str]], asks: List[List[str]]) -> Optional[Dict[str, float]]:
        """计算 spread / mid / wmid / depth / imbalance

        Args:
            bids: [[price, size], ...] 按价格降序
            asks: [[price, size], ...] 按价格升序

        Returns:
            dict or None
        """
        if not bids or not asks:
            return None

        best_bid = Decimal(bids[0][0])
        best_ask = Decimal(asks[0][0])
        spread = best_ask - best_bid
        mid = (best_ask + best_bid) / 2

        if mid == 0:
            return None

        wmid = OrderBookFactorCalculator._wmid(bids, asks)

        bid_depth_5 = OrderBookFactorCalculator._depth(bids, 5)
        ask_depth_5 = OrderBookFactorCalculator._depth(asks, 5)
        bid_depth_10 = OrderBookFactorCalculator._depth(bids, 10)
        ask_depth_10 = OrderBookFactorCalculator._depth(asks, 10)

        imbalance_5 = OrderBookFactorCalculator._imbalance(bids, asks, 5)
        imbalance_10 = OrderBookFactorCalculator._imbalance(bids, asks, 10)

        return {
            "spread": float(spread),
            "mid": float(mid),
            "wmid": float(wmid) if wmid is not None else None,
            "bid_depth_5": bid_depth_5,
            "ask_depth_5": ask_depth_5,
            "bid_depth_10": bid_depth_10,
            "ask_depth_10": ask_depth_10,
            "imbalance_5": imbalance_5,
            "imbalance_10": imbalance_10,
        }

    @staticmethod
    def _wmid(bids: List[List[str]], asks: List[List[str]]) -> Optional[Decimal]:
        if not bids or not asks:
            return None
        try:
            bid_px = Decimal(bids[0][0])
            bid_sz = Decimal(bids[0][1])
            ask_px = Decimal(asks[0][0])
            ask_sz = Decimal(asks[0][1])
            total = bid_sz + ask_sz
            if total == 0:
                return None
            return (bid_px * ask_sz + ask_px * bid_sz) / total
        except Exception:
            return None

    @staticmethod
    def _depth(side: List[List[str]], levels: int) -> float:
        total = 0.0
        for item in side[:levels]:
            try:
                total += float(item[1])
            except (ValueError, TypeError, IndexError):
                pass
        return total

    @staticmethod
    def _imbalance(bids: List[List[str]], asks: List[List[str]], levels: int) -> Optional[float]:
        bid_vol = OrderBookFactorCalculator._depth(bids, levels)
        ask_vol = OrderBookFactorCalculator._depth(asks, levels)
        total = bid_vol + ask_vol
        if total == 0:
            return None
        return (bid_vol - ask_vol) / total
