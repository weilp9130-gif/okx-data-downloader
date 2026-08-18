"""动态IP代理池 - 每次下载前自动发现/测试/应用可用节点

背景
----
VPN服务商节点不稳定：节点名、出口IP随时可能变化。因此不能写死节点和IP，
必须在每次下载K线前动态执行：

  1. 发现节点     —— 从 Clash/Mihomo API 读取当前真实节点
  2. 测试出口IP   —— 切换策略组逐个探测节点出口IP（可选缓存加速）
  3. 选独立IP     —— 每个独立IP选1个代表节点
  4. 生成listeners —— 为每个节点生成独立本地端口（Mihomo listeners，
                    流量绕过规则直走该节点，实现并发多IP）
  5. 应用到Clash  —— 写入 Verge 全局 Merge.yaml，提示重启内核，
                    自动轮询端口就绪
  6. 验证端口IP   —— 探测每个端口出口IP并去重
  7. 构建代理池   —— 用可用端口构建 ProxyPool（每IP独立限速）

用法（download_all.py 内置）:
    python download_all.py --dynamic --pool-size 20

或用命令行直接构建:
    python -m dynamic_pool --pool-size 10 --fresh
"""

import json
import os
import socket
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from .proxy_pool import ProxyPool, SKIP_TYPES, SKIP_PREFIXES, get_exit_ip
from ..utils.logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RUNTIME_DIR = PROJECT_ROOT / "runtime"
CACHE_FILE = RUNTIME_DIR / "dynamic_pool_cache.json"
LISTENERS_FILE = RUNTIME_DIR / "mihomo_listeners.yaml"
DEFAULT_API = "http://127.0.0.1:9097"
DEFAULT_PROXY_URL = "http://127.0.0.1:7890"
DEFAULT_BASE_PORT = 7891
SETTLE_SECONDS = 1.2

MARK_START = "# ===== OKX proxy pool: auto-generated, do not edit ====="
MARK_END = "# ===== end OKX proxy pool ====="


# ----------------------------------------------------------------------
# 发现与测试
# ----------------------------------------------------------------------
def discover_nodes(api: str = DEFAULT_API) -> List[str]:
    """从 Clash API 获取当前所有真实节点"""
    resp = requests.get(f"{api}/proxies", timeout=5)
    resp.raise_for_status()
    data = resp.json().get("proxies", {})
    nodes = []
    for name, info in data.items():
        if info.get("type") in SKIP_TYPES:
            continue
        if name.startswith(SKIP_PREFIXES):
            continue
        nodes.append(name)
    return nodes


def _mutable_groups(api: str) -> Dict[str, dict]:
    """所有可切换的策略组: {组名: {all: [节点/组列表]}}"""
    resp = requests.get(f"{api}/proxies", timeout=5)
    resp.raise_for_status()
    data = resp.json().get("proxies", {})
    return {
        name: info for name, info in data.items()
        if info.get("type") in ("Selector", "URLTest", "Fallback", "LoadBalance")
    }


def test_node_ips(
    api: str = DEFAULT_API,
    proxy_url: str = DEFAULT_PROXY_URL,
    nodes: Optional[List[str]] = None,
    settle: float = SETTLE_SECONDS,
    max_distinct: int = 0,
) -> Dict[str, str]:
    """逐个切换策略组测试节点出口IP: {节点名: 出口IP(失败为空串)}

    为保证流量一定经过被测节点，会把所有"包含该节点"的策略组
    全部切换到该节点；测试完成后恢复所有策略组原始选择。

    max_distinct>0 时提前停止：已测到该数量的独立出口IP即返回，
    用于减少"每次下载前测试"的耗时。
    """
    all_nodes = nodes if nodes else discover_nodes(api)
    groups = _mutable_groups(api)
    snapshot = {g: info.get("now", "") for g, info in groups.items()}

    results: Dict[str, str] = {}
    seen_ips = set()
    try:
        for node in all_nodes:
            for gname, ginfo in groups.items():
                if node in ginfo.get("all", []):
                    try:
                        requests.put(
                            f"{api}/proxies/{gname}", json={"name": node},
                            timeout=5,
                        )
                    except Exception:
                        pass
            time.sleep(settle)
            ip = get_exit_ip(proxy_url)
            results[node] = ip
            if ip:
                seen_ips.add(ip)
            if max_distinct > 0 and len(seen_ips) >= max_distinct:
                break
    finally:
        for gname, prev in snapshot.items():
            if prev:
                try:
                    requests.put(
                        f"{api}/proxies/{gname}", json={"name": prev}, timeout=5
                    )
                except Exception:
                    pass
    return results


# ----------------------------------------------------------------------
# 缓存
# ----------------------------------------------------------------------
def load_cache() -> dict:
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_cache(node_ips: Dict[str, str]) -> None:
    try:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "nodes": node_ips}, f,
                      ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存节点IP缓存失败: {e}")


def select_distinct_ips(node_ips: Dict[str, str], size: int) -> List[str]:
    """每个独立出口IP选1个代表节点，最多 size 个"""
    seen = set()
    picks = []
    for node in sorted(node_ips, key=lambda n: not bool(node_ips[n])):
        ip = node_ips.get(node, "")
        if not ip:
            continue
        if ip in seen:
            continue
        seen.add(ip)
        picks.append(node)
        if len(picks) >= size:
            break
    return picks


# ----------------------------------------------------------------------
# listeners 生成与应用
# ----------------------------------------------------------------------
def _yaml_str(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def listeners_yaml(nodes: List[str], base_port: int = DEFAULT_BASE_PORT,
                   listen: str = "127.0.0.1") -> str:
    lines = ["listeners:"]
    for i, node in enumerate(nodes):
        port = base_port + i
        lines.append(f"  - name: ip-pool-{port}")
        lines.append("    type: mixed")
        lines.append(f"    port: {port}")
        lines.append(f"    listen: {listen}")
        lines.append(f"    proxy: {_yaml_str(node)}")
    return "\n".join(lines)


def verge_merge_path() -> Optional[Path]:
    """Clash Verge 全局 Merge.yaml 路径（存在才返回）"""
    base = os.environ.get("APPDATA", "")
    if not base:
        return None
    p = Path(base) / "io.github.clash-verge-rev.clash-verge-rev" \
        / "profiles" / "Merge.yaml"
    return p if p.exists() else None


def _atomic_write(path: Path, content: str) -> None:
    """原子写文件：先写临时文件再替换，避免中途失败截断配置文件"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def apply_to_merge(yaml_text: str, merge_path: Optional[str] = None) -> bool:
    """把 listeners 写入 Verge 全局 Merge.yaml（保留已有内容，覆盖旧块）

    只替换 [MARK_START:] 之后本工具写入的内容（即使旧块不完整），
    之前的用户配置原样保留；原子写入防止截断损坏配置。
    """
    path = Path(merge_path) if merge_path else verge_merge_path()
    if path is None:
        logger.warning("未找到 Clash Verge 的 Merge.yaml，请手动粘贴生成的配置")
        return False
    block = f"{MARK_START}\n{yaml_text}\n{MARK_END}"
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if MARK_START in existing:
        pre = existing.split(MARK_START)[0]
        new = pre.rstrip() + "\n" + block
        if existing.endswith("\n"):
            new += "\n"
    else:
        new = existing.rstrip() + "\n" + block + "\n"
    _atomic_write(path, new)
    logger.info(f"已写入 listeners 到 {path}")
    return True


def _port_open(url: str, timeout: float = 1.5) -> bool:
    try:
        host, _, port = url.replace("http://", "").replace("https://", "").partition(":")
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


def wait_ports_ready(urls: List[str], timeout: float = 120.0) -> bool:
    """轮询所有监听端口是否就绪（等待 Clash 内核重启完成）"""
    start = time.time()
    while time.time() - start < timeout:
        if all(_port_open(u) for u in urls):
            return True
        time.sleep(2)
    return False


def verify_port_ips(urls: List[str], timeout: float = 10.0) -> List[Tuple[str, str]]:
    """探测每个端口出口IP: [(url, ip)]，失败ip为空串"""
    out = []
    for u in urls:
        out.append((u, get_exit_ip(u, timeout)))
    return out


# ----------------------------------------------------------------------
# 完整构建流程
# ----------------------------------------------------------------------
def build_dynamic_pool(
    pool_size: int = 20,
    ttl: int = 0,
    api: str = None,
    proxy_url: str = None,
    base_port: int = DEFAULT_BASE_PORT,
    listen: str = "127.0.0.1",
    per_ip_rate: Optional[int] = None,
    merge_path: Optional[str] = None,
    apply_timeout: float = 120.0,
    interactive: bool = True,
    no_apply: bool = False,
) -> Tuple[ProxyPool, Dict[str, str]]:
    """动态构建代理池

    Returns:
        (ProxyPool, {节点名: 端口URL})  端口URL可直接写入 OKX_PROXY_URLS 复用
    """
    api = api or DEFAULT_API
    proxy_url = proxy_url or DEFAULT_PROXY_URL

    # 1. 发现节点
    try:
        nodes = discover_nodes(api)
    except Exception as e:
        raise RuntimeError(f"无法连接 Clash API {api}: {e}")
    if not nodes:
        raise RuntimeError("Clash 中未发现任何真实节点")
    logger.info(f"发现节点 {len(nodes)} 个")

    # 2. 测试出口IP（支持缓存）
    cache = load_cache()
    use_cache = (
        ttl > 0 and cache.get("nodes")
        and (time.time() - cache.get("ts", 0)) < ttl
    )
    if use_cache:
        node_ips = cache["nodes"]
        ts = cache.get("ts", 0)
        logger.info(
            f"使用节点IP缓存（{time.strftime('%H:%M:%S', time.localtime(ts))}，"
            f"TTL={ttl}s），共 {len(node_ips)} 个"
        )
    else:
        logger.info("开始逐个测试节点出口IP（可加 --pool-ttl 复用缓存加速）...")
        node_ips = test_node_ips(api, proxy_url, nodes,
                                 max_distinct=pool_size * 2)
        ok = sum(1 for ip in node_ips.values() if ip)
        save_cache(node_ips)
        logger.info(f"节点测试完成: 可用 {ok}/{len(node_ips)}"
                    f"{'（已提前停止，够用为止）' if len(node_ips) < len(nodes) else ''}")

    # 3. 选独立IP
    picks = select_distinct_ips(node_ips, pool_size)
    if not picks:
        raise RuntimeError("没有可用节点，请检查代理/节点是否正常")
    logger.info(f"选出 {len(picks)} 个独立出口IP的节点，端口 "
                f"{base_port}~{base_port + len(picks) - 1}")

    # 4. 生成并应用 listeners
    urls = [f"http://127.0.0.1:{base_port + i}" for i in range(len(picks))]
    yaml_text = listeners_yaml(picks, base_port, listen)
    try:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        LISTENERS_FILE.write_text(yaml_text + "\n", encoding="utf-8")
    except Exception:
        pass

    # 记录Merge.yaml原始内容，失败时回滚，避免留下改坏的全局配置
    merge_file = Path(merge_path) if merge_path else verge_merge_path()
    original_merge = ""
    if merge_file and merge_file.exists():
        try:
            original_merge = merge_file.read_text(encoding="utf-8")
        except Exception:
            original_merge = ""

    def restore_merge() -> None:
        if merge_file and original_merge:
            try:
                _atomic_write(merge_file, original_merge)
                logger.warning("已还原 Merge.yaml 到本次修改前的状态")
            except Exception:
                logger.warning("还原 Merge.yaml 失败，请手动检查该文件")

    if no_apply:
        logger.info("干跑模式(no_apply): 仅生成配置，未写入 Merge.yaml")
        logger.info(f"生成的配置已保存到 {LISTENERS_FILE}")
        picks_only = list(picks)
        dummy = {n: f"http://127.0.0.1:{base_port + i}"
                 for i, n in enumerate(picks_only)}
        return None, dummy
    applied = apply_to_merge(yaml_text, merge_path)

    # 5. 等待内核重启 / 端口就绪
    if applied:
        logger.info("等待 Clash 内核重启使 listeners 生效...")
        logger.info("  若未自动重启，请右键托盘图标 → 重启内核（或重启 Clash Verge）")
        if not wait_ports_ready(urls, apply_timeout):
            if interactive:
                try:
                    input("端口未在限时内就绪。重启完成后按回车继续...")
                except EOFError:
                    restore_merge()
                    raise RuntimeError(
                        "端口未在限时内就绪（非交互环境），已还原 Merge.yaml，"
                        "请重启 Clash 内核后重试"
                    )
                wait_ports_ready(urls, 60)
            else:
                restore_merge()
                raise RuntimeError(
                    f"等待 {len(urls)} 个监听端口就绪超时，"
                    f"已还原 Merge.yaml，请重启 Clash 内核后重试"
                )

    # 6. 验证端口出口IP并去重
    verified = verify_port_ips(urls)
    good = []
    seen_ips = set()
    for u, ip in verified:
        if ip and ip not in seen_ips:
            seen_ips.add(ip)
            good.append(u)
    logger.info(f"端口验证: 可用 {len(good)}/{len(urls)} | "
                f"独立IP {len(seen_ips)} 个")
    if not good:
        restore_merge()
        raise RuntimeError("所有端口验证失败，已还原 Merge.yaml，无法构建代理池")

    # 7. 构建代理池
    pool = ProxyPool(proxies=good, per_ip_rate=per_ip_rate)
    port_map = {picks[i]: urls[i] for i in range(len(picks)) if urls[i] in good}
    return pool, port_map


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="动态IP代理池构建")
    p.add_argument("--pool-size", type=int, default=20, help="独立IP数量上限")
    p.add_argument("--pool-ttl", type=int, default=0,
                   help="复用节点IP缓存秒数(0=每次重测)")
    p.add_argument("--fresh", action="store_true", help="强制重新测试(忽略缓存)")
    p.add_argument("--base-port", type=int, default=DEFAULT_BASE_PORT)
    p.add_argument("--api", default=None)
    p.add_argument("--proxy-url", default=None)
    p.add_argument("--per-ip-rate", type=int, default=None)
    p.add_argument("--apply-timeout", type=float, default=120.0)
    p.add_argument("--no-prompt", action="store_true")
    args = p.parse_args()

    from utils.logger import setup_logging
    setup_logging(level="INFO", file_enabled=False)

    pool, port_map = build_dynamic_pool(
        pool_size=args.pool_size,
        ttl=args.pool_ttl,
        api=args.api,
        proxy_url=args.proxy_url,
        base_port=args.base_port,
        per_ip_rate=args.per_ip_rate,
        apply_timeout=args.apply_timeout,
        interactive=not args.no_prompt,
    )
    print(f"\n✅ 动态代理池构建完成: {pool.stats()}")
    print(f"   OKX_PROXY_URLS={','.join(p.proxies['https'] for p in port_map.values())}")
