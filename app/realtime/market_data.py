"""WebSocket 市场数据处理器（OI / Funding / Mark / Index / Kline）

Phase 7：处理 open-interest、funding-rate、mark-price、index-tickers
以及 candle{bar} 频道的 WS 消息。
"""

import re
from datetime import datetime, timezone
from typing import Optional

from ..utils.logger import get_logger
from ..utils.time_utils import ms_to_datetime

logger = get_logger(__name__)

CANDLE_CHANNEL_RE = re.compile(r"^candle(\S+)$")


class MarketDataHandler:
    """处理 OKX 市场数据频道 WS 消息

    返回按写入目标分组的记录列表：
        [{"target": "open_interest_realtime", "record": {...}},
         {"target": "funding_rates", "record": {...}}, ...]
    """

    def handle(self, data: dict) -> list:
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        if not channel:
            return []

        raw_data = data.get("data", [])
        if not raw_data:
            return []

        received_at = datetime.now(timezone.utc)

        if channel == "open-interest":
            return self._handle_open_interest(raw_data, received_at)
        if channel == "funding-rate":
            return self._handle_funding_rate(raw_data, received_at)
        if channel == "mark-price":
            return self._handle_mark_price(raw_data, received_at)
        if channel == "index-tickers":
            return self._handle_index_tickers(raw_data, received_at)
        m = CANDLE_CHANNEL_RE.match(channel)
        if m:
            return self._handle_candles(raw_data, m.group(1), received_at)

        return []

    # ------------------------------------------------------------------
    def _handle_open_interest(self, raw_list: list, received_at: datetime) -> list:
        records = []
        for raw in raw_list:
            inst_id = raw.get("instId")
            ts_raw = raw.get("ts")
            if not inst_id or not ts_raw:
                continue
            try:
                ts = ms_to_datetime(int(ts_raw))
            except (ValueError, TypeError):
                continue
            records.append({
                "target": "open_interest_realtime",
                "record": {
                    "inst_id": inst_id,
                    "ts": ts,
                    "oi": raw.get("oi"),
                    "oi_ccy": raw.get("oiCcy"),
                    "oi_usd": raw.get("oiUsd"),
                    "raw_json": raw,
                    "received_at": received_at,
                    "ingested_at": datetime.now(timezone.utc),
                },
            })
        return records

    # ------------------------------------------------------------------
    def _handle_funding_rate(self, raw_list: list, received_at: datetime) -> list:
        records = []
        for raw in raw_list:
            inst_id = raw.get("instId")
            funding_time_raw = raw.get("fundingTime")
            if not inst_id or not funding_time_raw:
                continue
            try:
                ts = ms_to_datetime(int(funding_time_raw))
            except (ValueError, TypeError):
                continue
            records.append({
                "target": "funding_rates",
                "record": {
                    "inst_id": inst_id,
                    "ts": ts,
                    "funding_rate": raw.get("fundingRate"),
                    "realized_rate": raw.get("realizedRate"),
                    "funding_time": ts,
                },
            })
        return records

    # ------------------------------------------------------------------
    def _handle_mark_price(self, raw_list: list, received_at: datetime) -> list:
        records = []
        for raw in raw_list:
            inst_id = raw.get("instId")
            ts_raw = raw.get("ts")
            mark_px = raw.get("markPx")
            if not inst_id or not ts_raw or mark_px is None:
                continue
            try:
                ts = ms_to_datetime(int(ts_raw))
            except (ValueError, TypeError):
                continue
            records.append({
                "target": "mark_prices",
                "record": {
                    "inst_id": inst_id,
                    "bar": "realtime",
                    "ts": ts,
                    "o": mark_px,
                    "h": mark_px,
                    "l": mark_px,
                    "c": mark_px,
                    "source": "WS",
                    "received_at": received_at,
                    "fetched_at": received_at,
                    "raw_json": raw,
                    "ingested_at": datetime.now(timezone.utc),
                },
            })
        return records

    # ------------------------------------------------------------------
    def _handle_index_tickers(self, raw_list: list, received_at: datetime) -> list:
        records = []
        for raw in raw_list:
            inst_id = raw.get("instId")
            ts_raw = raw.get("ts")
            idx_px = raw.get("idxPx")
            if not inst_id or not ts_raw or idx_px is None:
                continue
            try:
                ts = ms_to_datetime(int(ts_raw))
            except (ValueError, TypeError):
                continue
            records.append({
                "target": "index_prices",
                "record": {
                    "inst_id": inst_id,
                    "bar": "realtime",
                    "ts": ts,
                    "o": idx_px,
                    "h": idx_px,
                    "l": idx_px,
                    "c": idx_px,
                    "source": "WS",
                    "received_at": received_at,
                    "fetched_at": received_at,
                    "raw_json": raw,
                    "ingested_at": datetime.now(timezone.utc),
                },
            })
        return records

    # ------------------------------------------------------------------
    def _handle_candles(self, raw_list: list, bar: str, received_at: datetime) -> list:
        """处理 candle{bar} 频道（数组格式 [ts, o, h, l, c, vol, ...]）

        注：需依赖 WS 消息 arg 中的 instId，data 项为纯数组。
        """
        records = []
        # candle 频道的 instId 来自 arg，需要由调用方注入
        return records

    def handle_candles_with_inst(
        self, data: dict, bar: str, inst_id: str, received_at: datetime
    ) -> list:
        """带 instId 的 K线处理（由 manager 在知道 arg.instId 时调用）"""
        raw_list = data.get("data", [])
        records = []
        for raw in raw_list:
            if not isinstance(raw, list) or len(raw) < 6:
                continue
            try:
                ts = ms_to_datetime(int(raw[0]))
            except (ValueError, TypeError):
                continue
            records.append({
                "target": "candles",
                "record": {
                    "inst_id": inst_id,
                    "bar": bar,
                    "ts": ts,
                    "o": raw[1],
                    "h": raw[2],
                    "l": raw[3],
                    "c": raw[4],
                    "vol": raw[5],
                    "vol_ccy": raw[6] if len(raw) > 6 else None,
                    "vol_ccy_quote": raw[7] if len(raw) > 7 else None,
                    "confirm": raw[8] if len(raw) > 8 else None,
                },
            })
        return records
