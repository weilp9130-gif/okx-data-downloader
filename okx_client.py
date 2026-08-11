"""OKX交易所API客户端

使用 requests 直接调用 OKX 公开 REST API（V5），
不依赖官方 SDK，稳定可靠。

功能：
- 获取K线数据（spot & swap）
- 获取资金费率（funding rate）
- 获取交易对（Instruments）信息
- 内置全局限速 / 指数退避重试 / 代理支持

数据格式参考：
GET /api/v5/market/candles  → data为二维数组
每行: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
"""

import os
import threading
import time
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Config
from utils.logger import get_logger

logger = get_logger(__name__)


class OKXClient:
    """OKX公开行情数据客户端（requests实现）

    - 复用 requests.Session（HTTP keep-alive），避免每次请求重新 TLS 握手，大幅提速
    - 连接池支持多线程并发下载
    """

    TIMEOUT = 15
    MAX_RETRIES = 5

    def __init__(self):
        self.cfg = Config().okx
        self.BASE_URL = self.cfg.base_url

        # 代理设置：优先使用显式配置的 PROXY_URL；
        # 否则信任环境变量/系统代理（部分网络环境必须走代理才能访问OKX）
        self.proxies = self._resolve_proxy()

        # 全局请求限速器状态（令牌桶）
        self._rate_lock = threading.Lock()
        self._rate = max(self.cfg.rate_limit_per_second, 1)
        self._tokens = float(self._rate)            # 当前可用令牌
        self._last_refill = time.monotonic()        # 上次补充令牌时间
        self._max_rate = self._rate                 # 初始最大速率
        self._cool_until = 0.0                      # 429冷却截止时间

        # 复用连接的 Session（keep-alive）+ 连接池，支持多线程并发下载
        # 每个线程独立 Session（thread-local），避免多线程共享 session 竞争
        self._tl = threading.local()
        self._adapter = HTTPAdapter(
            pool_connections=32,   # 不同主机最大连接数
            pool_maxsize=32,       # 单主机最大连接池（避免过多连接触发限速）
            max_retries=Retry(
                total=self.MAX_RETRIES,
                backoff_factor=1.0,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                raise_on_status=False,
            ),
        )

    def _get_session(self):
        """获取当前线程专属的 requests.Session（懒创建复用）"""
        if not hasattr(self._tl, "session"):
            s = requests.Session()
            s.headers.update({"User-Agent": "Mozilla/5.0"})
            if self.proxies:
                s.proxies.update(self.proxies)
            s.mount("https://", self._adapter)
            s.mount("http://", self._adapter)
            self._tl.session = s
        return self._tl.session

    def close(self):
        """关闭所有线程创建的底层连接（程序结束时调用）"""
        # 关闭当前线程的 session（其余线程 session 随线程结束被GC回收）
        try:
            s = getattr(self._tl, "session", None)
            if s is not None:
                s.close()
        except Exception:
            pass

    def _resolve_proxy(self):
        """解析代理配置（预留功能）"""
        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            return {"http": proxy_url, "https": proxy_url}
        return None

    # ------------------------------------------------------------------
    # 请求限速
    # ------------------------------------------------------------------
    def _throttle(self) -> None:
        """全局令牌桶限速（多线程安全）

        每秒最多发放 rate 个令牌，每个请求消耗1个令牌。
        令牌按速率持续补充，严格限制整体请求频率，
        避免触发 OKX 对 history-candles 的频控(429)。
        """
        while True:
            now = time.monotonic()
            with self._rate_lock:
                self._tokens = min(
                    self._rate,
                    self._tokens + (now - self._last_refill) * self._rate,
                )
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            if wait > 0:
                time.sleep(wait)

    def _on_429(self) -> None:
        """收到429后：临时冷却，强制降低请求速率，避免持续被限流"""
        with self._rate_lock:
            # 降低到当前速率的一半，最低不低于1
            new_rate = max(1, int(self._rate * 0.5))
            self._rate = new_rate
            # 清空令牌，停止立即放行
            self._tokens = 0.0
            self._cool_until = time.monotonic() + 5.0

    def _throttle_after_429(self) -> None:
        """429冷却等待：在冷却期内阻塞所有请求"""
        while True:
            now = time.monotonic()
            with self._rate_lock:
                if now >= self._cool_until:
                    return
                wait = self._cool_until - now
            time.sleep(min(wait, 1.0))

    def _is_retryable(self, exc: Exception) -> bool:
        """判断异常是否值得重试（429 由 _get 中的专门降速逻辑处理）"""
        if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
            return True
        if isinstance(exc, requests.HTTPError):
            status = exc.response.status_code if exc.response is not None else 0
            return status in (500, 502, 503, 504)
        return False

    def _get(self, path: str, params: dict = None) -> List[dict]:
        """向OKX发起GET请求，返回data列表

        Args:
            path: API路径，如 '/api/v5/market/candles'
            params: URL参数

        Returns:
            List[dict]: 响应的data内容

        Raises:
            RuntimeError: OKX业务错误或请求最终失败
        """
        last_exc = None
        url = self.BASE_URL + path

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self._throttle()
                resp = self._get_session().get(
                    url,
                    params=params,
                    timeout=self.TIMEOUT,
                    proxies=self.proxies,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
                js = resp.json()

                code = js.get("code", "0")
                if code not in ("0", 0, None):
                    raise RuntimeError(
                        f"OKX API error: code={code}, msg={js.get('msg')}, path={path}"
                    )
                return js.get("data", [])

            except RuntimeError:
                raise   # OKX业务错误不重试
            except Exception as e:
                last_exc = e
                is_429 = (
                    isinstance(e, requests.HTTPError)
                    and e.response is not None
                    and e.response.status_code == 429
                )
                if is_429:
                    # 触发频控：降低限速并冷却，避免持续撞墙
                    self._on_429()
                    self._throttle_after_429()
                    if attempt >= self.MAX_RETRIES:
                        logger.warning(
                            "%s 持续429，重试次数用尽，跳过该请求", path)
                        break
                    wait = min(2 ** (attempt - 1), 30)
                    logger.warning(
                        "429频控，已降速至 %d/s，%ds后重试(%d/%d): %s",
                        self._rate, wait, attempt, self.MAX_RETRIES, path,
                    )
                    time.sleep(wait)
                    continue
                if attempt < self.MAX_RETRIES and self._is_retryable(e):
                    wait = min(2 ** (attempt - 1), 30)   # 1s, 2s, 4s...
                    logger.warning(
                        "请求失败，%ds后重试(%d/%d): %s %s",
                        wait, attempt, self.MAX_RETRIES, path, e,
                    )
                    time.sleep(wait)
                else:
                    break

        # 网络连接类失败时，附加代理/网络诊断提示
        import socket
        is_conn_err = isinstance(
            last_exc,
            (requests.ConnectionError, requests.ProxyError, socket.gaierror),
        )
        hint = ""
        if is_conn_err:
            hint = ("\n提示: 无法连接到OKX。若需代理访问，请在环境变量 PROXY_URL "
                    "设置代理地址(如 http://127.0.0.1:7890)，或检查网络/代理是否可用。")
        raise RuntimeError(
            f"请求最终失败(已重试{self.MAX_RETRIES}次): {path} - {last_exc}{hint}"
        ) from last_exc

    # ------------------------------------------------------------------
    # K线数据
    # ------------------------------------------------------------------
    def get_candles(
        self,
        inst_id: str,
        bar: str = "1m",
        after: Optional[int] = None,
        before: Optional[int] = None,
        limit: int = 100,
    ) -> List[dict]:
        """获取K线数据

        Args:
            inst_id: 产品ID，如 ETH-USDT-SWAP / BTC-USDT
            bar: 时间粒度
            after: 请求此时间戳之前(更旧)的数据
            before: 请求此时间戳之后(更新)的数据
            limit: 请求数量，最大100（历史接口最大100，普通接口最大300）

        Returns:
            List[dict]: 转换后的K线字典列表
        """
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(min(limit, 100)),
        }
        if after:
            params["after"] = str(after)
        if before:
            params["before"] = str(before)

        data = self._get("/api/v5/market/history-candles", params)

        candles = []
        for d in data:
            candles.append(
                {
                    "ts": int(d[0]),
                    "o": d[1],
                    "h": d[2],
                    "l": d[3],
                    "c": d[4],
                    "vol": d[5],
                    "vol_ccy": d[6] if len(d) > 6 else None,
                    "vol_ccy_quote": d[7] if len(d) > 7 else None,
                    "confirm": d[8] if len(d) > 8 else None,
                }
            )
        return candles

    # ------------------------------------------------------------------
    # 资金费率
    # ------------------------------------------------------------------
    def get_funding_rate(
        self,
        inst_id: str,
        after: Optional[int] = None,
        before: Optional[int] = None,
        limit: int = 100,
    ) -> List[dict]:
        """获取资金费率历史

        Args:
            inst_id: 合约产品ID，如 ETH-USDT-SWAP
            after: 请求此时间戳之前的数据
            before: 请求此时间戳之后的数据
            limit: 请求数量，最大100

        Returns:
            List[dict]: 资金费率字典列表
        """
        params = {
            "instId": inst_id,
            "limit": str(min(limit, 100)),
        }
        if after:
            params["after"] = str(after)
        if before:
            params["before"] = str(before)

        data = self._get("/api/v5/public/funding-rate-history", params)

        rates = []
        for d in data:
            rates.append(
                {
                    # OKX资金费率接口用 fundingTime 作为结算时间戳
                    "ts": int(d.get("fundingTime", 0)),
                    "funding_rate": d.get("fundingRate"),
                    "realized_rate": d.get("realizedRate"),
                    "funding_time": d.get("fundingTime"),
                }
            )
        return rates

    # ------------------------------------------------------------------
    # 交易对信息
    # ------------------------------------------------------------------
    def get_instruments(self, inst_type: str = "SWAP") -> List[dict]:
        """获取交易对列表

        Args:
            inst_type: SPOT / SWAP / FUTURES / OPTION

        Returns:
            List[dict]: 交易对信息列表
        """
        data = self._get("/api/v5/public/instruments", {"instType": inst_type})
        return data

    # ------------------------------------------------------------------
    # 当前实时资金费率
    # ------------------------------------------------------------------
    def get_current_funding_rate(self, inst_id: str) -> Optional[dict]:
        """获取当前资金费率

        Args:
            inst_id: 合约ID

        Returns:
            dict 或 None
        """
        data = self._get("/api/v5/public/funding-rate", {"instId": inst_id})
        if not data:
            return None
        d = data[0]
        return {
            "ts": int(d.get("fundingTime", 0)),
            "funding_rate": d.get("fundingRate"),
            "next_funding_time": d.get("nextFundingRate"),
        }
