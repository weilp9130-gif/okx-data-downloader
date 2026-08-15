"""IP代理池 - 为并发下载分配多个出口IP，打破单IP限速瓶颈

背景
----
OKX 公开行情接口(history-candles)按"IP"限频(约 20 请求/2秒/IP)。
旧版 OKXClient 使用"全局限速器"，所有线程共享一个令牌桶，
整体吞吐被锁死在单个 IP 的额度内(默认 16 req/s)。

方案
----
为每个币(合约)绑定一个独立代理/出口IP，每个IP拥有独立的限速令牌桶。
N 个独立IP 即可获得约 N 倍的吞吐(一个币一个IP)。

代理来源(按优先级)：
  1. OKX_PROXY_URLS      环境变量：逗号分隔的代理URL列表
     （多个出口IP的代理直接写在这里，如 http://ip1:port,http://ip2:port）
  2. OKX_PROXY_LIST_FILE 文件路径：每行一个代理URL（支持 # 注释）
  3. Clash/Mihomo 自动发现：从 CLASH_API_URL 读取真实节点
     ⚠️ 注意：Clash 混合端口(如 7890)所有流量都走"当前选中节点"，
        仅能同时使用1个出口IP。若节点是独立出口，需为每个节点
        配置独立监听端口(Mihomo listeners)，再用 OKX_PROXY_URLS 填入。

用法
----
    from proxy_pool import ProxyPool
    pool = ProxyPool()
    pool.verify_ips()              # 可选：探测每个代理出口IP并统计去重
    proxy = pool.acquire("BTC-USDT-SWAP")   # 获取绑定代理(内部已按IP限速)
    try:
        resp = requests.get(url, proxies=proxy.proxies, timeout=15)
        pool.report_ok(proxy)
    except Exception as e:
        pool.report_fail(proxy, is_429=(429 in ...))
"""

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from concurrent.futures import ThreadPoolExecutor

from .config import Config
from .utils.logger import get_logger

logger = get_logger(__name__)

IP_CHECK_URL = "https://ifconfig.me/ip"

# Clash 中"策略组"类型，不是真实节点，跳过
SKIP_TYPES = {
    "Selector", "URLTest", "LoadBalance", "Direct", "Reject",
    "Fallback", "Compatible", "Pass", "RejectDrop", "Relay",
}
SKIP_PREFIXES = ("剩余流量", "套餐到期", "官网", "[D]", "♻️", "♻", "🏁", "流量")

# 代理失败后的禁用时长
COOL_429_SECONDS = 15.0
DISABLE_FAIL_SECONDS = 60.0
STALL_MAX_NONE = 30          # acquire 连续取不到代理的最大次数(每次sleep2s)
ACQUIRE_NONE_SLEEP = 2.0


class _TokenBucket:
    """单代理(IP)平滑限速器，线程安全

    实测结论（2026-08，干净环境基准）：
    OKX history-candles 按 IP 限频约 20 请求/2秒。旧版令牌桶实现是
    "攒令牌→多线程瞬间抽干"，请求成簇突发，轻松打爆 2 秒窗口触发大量
    429，导致冷却+重试恶性循环（同配置下 8 IP 仅 8.7 pages/s + 1548 次
    429）。本实现改为"下次时隙预留"：严格按 1/rate 秒间隔发放请求，
    无突发。实测 16 IP × 每 IP 2 线程 × rate=8 时达到 ~73 pages/s
    （约7300根K线/秒）且 429 几乎为零。
    """

    def __init__(self, rate: float):
        self.rate = max(float(rate), 1.0)
        self._lock = threading.Lock()
        self._next = 0.0            # 下一个允许发出请求的时刻(monotonic)
        self._cooldown_until = 0.0  # 429 冷却截止时刻

    def wait(self) -> None:
        """阻塞直到可以发出下一个请求（严格按 rate 间隔平滑发放）"""
        while True:
            now = time.monotonic()
            reserved = False
            with self._lock:
                if now < self._cooldown_until:
                    # 429 冷却中：仅等待冷却结束，不预留请求时隙
                    sleep_for = self._cooldown_until - now
                else:
                    # 预留下一个请求时隙（每次调用只预留一次）
                    slot = max(now, self._next)
                    self._next = slot + 1.0 / self.rate
                    sleep_for = slot - now
                    reserved = True
            if sleep_for > 0:
                time.sleep(sleep_for)
            if reserved:
                return

    def set_cooldown(self, seconds: float) -> None:
        with self._lock:
            self._cooldown_until = time.monotonic() + seconds
            # 冷却结束后从当前时刻重新开始平滑计时，避免突发补偿
            self._next = max(self._next, self._cooldown_until)


@dataclass(eq=False)
class Proxy:
    """单个代理条目"""
    url: str
    name: str = ""
    exit_ip: str = ""
    buckets: Optional[_TokenBucket] = None
    fails: int = 0
    uses: int = 0
    disable_until: float = 0.0

    @property
    def proxies(self) -> Dict[str, str]:
        return {"http": self.url, "https": self.url}

    def healthy(self, now: float = None) -> bool:
        now = now if now is not None else time.monotonic()
        return now >= self.disable_until


class ProxyPool:
    """IP代理池：分配、限速、健康管理、IP去重探测"""

    def __init__(self, proxies: List[str] = None, per_ip_rate: float = None):
        cfg = Config().okx
        self.per_ip_rate = per_ip_rate or cfg.ip_rate_limit_per_second
        self._lock = threading.Lock()
        self._proxies: List[Proxy] = []
        self._assigned: Dict[str, Proxy] = {}
        self._rr = 0
        self._load(proxies)

    # ------------------------------------------------------------------
    # 加载
    # ------------------------------------------------------------------
    def _load(self, proxies: Optional[List[str]]) -> None:
        urls = []
        if proxies:
            urls = [u for u in proxies if u and u.strip()]
        if not urls:
            urls = self._from_env_or_file()
        if not urls:
            urls = self._from_clash()

        seen = set()
        for u in urls:
            u = u.strip()
            if not u or u in seen:
                continue
            seen.add(u)
            self._proxies.append(
                Proxy(url=u, name=u, buckets=_TokenBucket(self.per_ip_rate))
            )

        if not self._proxies:
            logger.warning(
                "代理池为空：未配置任何代理。将退回直连+全局限速模式"
            )

    def _from_env_or_file(self) -> List[str]:
        cfg = Config().okx
        urls: List[str] = []
        if cfg.proxy_urls:
            urls += [u for u in cfg.proxy_urls.split(",") if u.strip()]
        if cfg.proxy_list_file:
            path = cfg.proxy_list_file
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        urls.append(line)
            except Exception as e:
                logger.warning(f"读取代理列表文件失败 {path}: {e}")
        return urls

    def _from_clash(self) -> List[str]:
        cfg = Config().okx
        api = cfg.clash_api_url
        if not api:
            return []
        try:
            resp = requests.get(f"{api}/proxies", timeout=5)
            resp.raise_for_status()
            data = resp.json().get("proxies", {})
            nodes = []
            for name, info in data.items():
                t = info.get("type", "")
                if t in SKIP_TYPES:
                    continue
                if name.startswith(SKIP_PREFIXES):
                    continue
                nodes.append(name)
            if not nodes:
                return []
            logger.warning(
                f"Clash 自动发现 {len(nodes)} 个节点，但混合端口({cfg.clash_proxy_url})"
                " 同时只能走1个出口IP。若需多IP并行，请为每个节点配置"
                " 独立监听端口后改用 OKX_PROXY_URLS 填写。"
            )
            return [cfg.clash_proxy_url]
        except Exception as e:
            logger.warning(f"Clash 自动发现代理失败: {e}")
            return []

    # ------------------------------------------------------------------
    # 分配
    # ------------------------------------------------------------------
    def acquire(self, key: str = None) -> Optional[Proxy]:
        """为 key(通常传币名/线程名)获取一个可用代理，并等待该代理限速令牌。

        同一 key 始终返回同一个代理(一个币一个IP)；代理失效时自动换新。
        池内无可用代理时返回 None（调用方应稍后重试）。
        """
        stall = 0
        while True:
            with self._lock:
                proxy = self._assigned.get(key) if key else None
                now = time.monotonic()
                if proxy is None or not proxy.healthy(now):
                    if proxy is not None:
                        del self._assigned[key]
                    proxy = self._pick_healthy_locked(now)
                    if proxy is None:
                        stall += 1
                        if stall >= STALL_MAX_NONE:
                            raise RuntimeError(
                                "代理池长时间无可用IP，请检查代理是否全部失效"
                            )
                        time.sleep(ACQUIRE_NONE_SLEEP)
                        continue
                    if key is not None:
                        self._assigned[key] = proxy
            proxy.buckets.wait()
            return proxy

    def _pick_healthy_locked(self, now: float) -> Optional[Proxy]:
        assigned = set(self._assigned.values())
        # 优先选"未被其他币占用"的健康代理 → 保证一个币一个IP
        for p in self._proxies:
            if p.healthy(now) and p not in assigned:
                return p
        # 池比币少：轮询一个健康代理
        n = len(self._proxies)
        for _ in range(n):
            self._rr = (self._rr + 1) % n
            p = self._proxies[self._rr]
            if p.healthy(now):
                return p
        return None

    def unassign(self, key: str) -> None:
        """释放某个 key 绑定的代理（任务结束时可调用，便于复用）"""
        with self._lock:
            self._assigned.pop(key, None)

    # ------------------------------------------------------------------
    # 健康上报
    # ------------------------------------------------------------------
    def report_ok(self, proxy: Proxy) -> None:
        with self._lock:
            proxy.fails = 0
            proxy.disable_until = 0.0
            proxy.uses += 1

    def report_fail(self, proxy: Proxy, is_429: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            proxy.fails += 1
            proxy.uses += 1
            if is_429:
                proxy.disable_until = now + COOL_429_SECONDS
                if proxy.buckets:
                    proxy.buckets.set_cooldown(COOL_429_SECONDS)
            elif proxy.fails >= 3:
                proxy.disable_until = now + DISABLE_FAIL_SECONDS
            else:
                proxy.disable_until = now + 5.0

    # ------------------------------------------------------------------
    # 出口IP探测（去重统计）
    # ------------------------------------------------------------------
    def verify_ips(self, timeout: float = 8.0) -> int:
        """并发探测每个代理的出口IP，返回独立IP数量

        ⚠️ 探测结果直接决定加速效果：若所有代理出口IP相同，
           则速率仍受单个IP限频约束，无法并行加速。
        """
        if not self._proxies:
            return 0

        def check(p: Proxy) -> Tuple[Proxy, str]:
            return p, get_exit_ip(p.url, timeout=timeout)

        with ThreadPoolExecutor(max_workers=min(len(self._proxies), 20)) as ex:
            results = list(ex.map(check, self._proxies))

        by_ip: Dict[str, List[Proxy]] = {}
        for p, ip in results:
            p.exit_ip = ip
            if ip:
                by_ip.setdefault(ip, []).append(p)

        ok = sum(1 for _, ip in results if ip)
        unique = len(by_ip)
        logger.info(
            f"代理池出口IP探测: 可用 {ok}/{len(self._proxies)} | "
            f"独立IP {unique} 个"
        )
        if unique <= 1 and ok > 0:
            logger.warning(
                "所有可用代理出口IP相同，无法通过并行突破单IP限频。"
                "请更换/购买多出口IP的代理，或用 Mihomo listeners 配置独立端口。"
            )
        return unique

    # ------------------------------------------------------------------
    # 状态
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._proxies)

    def healthy_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for p in self._proxies if p.healthy(now))

    def stats(self) -> str:
        now = time.monotonic()
        with self._lock:
            total = len(self._proxies)
            healthy = sum(1 for p in self._proxies if p.healthy(now))
            assigned = len(self._assigned)
            ips = len({p.exit_ip for p in self._proxies if p.exit_ip})
        return (f"代理池: 共{total}个(健康{healthy}) | 已分配{assigned} | "
                f"每IP限速{self.per_ip_rate:.0f}/s | 已探测独立IP {ips} 个")


def get_exit_ip(proxy_url: str, timeout: float = 8.0) -> str:
    """通过指定代理探测出口IP（文本返回），失败返回空串

    供 ProxyPool.verify_ips 与 dynamic_pool 共用，保证IP检测口径一致。
    """
    try:
        r = requests.get(
            IP_CHECK_URL,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return r.text.strip()
    except Exception:
        return ""


def build_proxy_pool(args) -> Optional[ProxyPool]:
    """按命令行参数构建IP代理池；未配置时返回None（直连+全局限速）

    优先级: --dynamic 动态池 > OKX_PROXY_URLS/文件 静态池 > 无池。
    main.py 与 download_all.py 共用本函数，避免两份逻辑漂移。
    """
    if args.dynamic:
        from .dynamic_pool import build_dynamic_pool
        logger.info("=" * 60)
        logger.info("动态IP代理池: 发现节点 → 测试IP → 应用listeners → 构建池")
        logger.info("=" * 60)
        pool, _ = build_dynamic_pool(
            pool_size=args.pool_size,
            ttl=args.pool_ttl,
            base_port=args.pool_base_port,
            per_ip_rate=args.per_ip_rate,
            interactive=not args.no_prompt,
        )
        if args.proxy_verify:
            pool.verify_ips()
        logger.info(pool.stats())
        return pool

    okx = Config().okx
    has_source = bool(
        okx.proxy_urls or okx.proxy_list_file or okx.clash_api_url
    )
    if not (args.proxy_pool or has_source):
        return None
    pool = ProxyPool(per_ip_rate=args.per_ip_rate)
    if len(pool) == 0:
        logger.warning("代理池为空：请设置 OKX_PROXY_URLS 或 "
                       "OKX_PROXY_LIST_FILE，本次退回直连+全局限速")
        return None
    if args.proxy_verify:
        pool.verify_ips()
    else:
        logger.info(pool.stats())
        logger.info("提示：加 --proxy-verify 可探测各代理真实出口IP去重统计")
    return pool


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="代理池出口IP检查")
    p.add_argument("--proxies", default=None,
                   help="逗号分隔的代理URL，覆盖环境变量配置")
    p.add_argument("--per-ip-rate", type=float, default=None,
                   help="每个IP的请求限速")
    args = p.parse_args()

    from utils.logger import setup_logging
    setup_logging(level="INFO", file_enabled=False)

    pool = ProxyPool(proxies=args.proxies.split(",") if args.proxies else None,
                     per_ip_rate=args.per_ip_rate)
    print(f"\n{pool.stats()}")
    if len(pool) == 0:
        print("未发现任何代理。请设置 OKX_PROXY_URLS 或 OKX_PROXY_LIST_FILE。")
    else:
        n = pool.verify_ips()
        print(f"独立出口IP数量: {n}\n")
        with pool._lock:
            for pr in pool._proxies:
                mark = pr.exit_ip or "-"
                print(f"  {pr.name:<40} -> {mark}")
