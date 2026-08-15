"""OKX 相关公共工具函数

集中维护交易对列表解析、listTime 转换等逻辑，避免在多个入口脚本中重复实现。
"""

from datetime import datetime, timezone
from typing import List, Optional

from .time_utils import ms_to_datetime


def ms_to_naive_utc(ms) -> Optional[datetime]:
    """毫秒时间戳 -> naive UTC datetime（供 listTime 使用）"""
    if ms is None:
        return None
    dt = ms_to_datetime(int(ms))
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def get_swap_contracts(client) -> List[dict]:
    """返回带 listTime 的 USDT 永续合约列表（仅 state=live）"""
    data = client.get_instruments('SWAP')
    contracts = []
    for d in data:
        if (d.get('instType') == 'SWAP'
                and d.get('settleCcy') == 'USDT'
                and d.get('state') == 'live'):
            contracts.append({'instId': d['instId'], 'listTime': d.get('listTime')})
    contracts.sort(key=lambda x: x['instId'])
    return contracts
