"""OKX 工具函数测试（离线）"""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.utils.okx_utils import ms_to_naive_utc, get_swap_contracts


class TestOKXUtils(unittest.TestCase):
    def test_ms_to_naive_utc(self):
        dt = ms_to_naive_utc(1621091600000)
        self.assertIsNotNone(dt)
        self.assertIsNone(dt.tzinfo)
        self.assertEqual(dt.year, 2021)

    def test_ms_to_naive_utc_none(self):
        self.assertIsNone(ms_to_naive_utc(None))

    def test_get_swap_contracts_filters_and_sorts(self):
        mock_client = MagicMock()
        mock_client.get_instruments.return_value = [
            {'instId': 'BTC-USDT-SWAP', 'instType': 'SWAP', 'settleCcy': 'USDT', 'state': 'live', 'listTime': '0'},
            {'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'settleCcy': 'USDT', 'state': 'live'},
            {'instId': 'LTC-USDT', 'instType': 'SPOT', 'settleCcy': 'USDT', 'state': 'live'},
            {'instId': 'BTC-USDT', 'instType': 'SPOT', 'settleCcy': 'USDT', 'state': 'live'},
        ]
        contracts = get_swap_contracts(mock_client)
        self.assertEqual([c['instId'] for c in contracts],
                         ['BTC-USDT-SWAP', 'ETH-USDT-SWAP'])


if __name__ == "__main__":
    unittest.main()
