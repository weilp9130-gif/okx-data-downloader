# OKX数据下载器

一个专业的Python量化项目，用于从 [OKX交易所](https://www.okx.com) 下载行情数据并存储到 **PostgreSQL + TimescaleDB**。

## 功能特性

- ✅ OKX API客户端封装（K线、资金费率、交易对信息）
- ✅ PostgreSQL + TimescaleDB 数据库连接（连接池管理）
- ✅ SQLAlchemy ORM数据模型（K线表、资金费率表，含TimescaleDB hypertable）
- ✅ K线数据批量下载模块（支持时间分段、增量更新、断点续传）
- ✅ 资金费率下载模块
- ✅ 可配置日志系统（控制台 + 文件轮转）
- ✅ 环境变量配置管理（`.env`）
- ✅ 请求限速 + 指数退避重试
- ✅ **全历史并行下载**（合约 × 时间窗，listTime 精确定位回溯起点）

## 项目结构

```
okx_data_downloader
├── app/                 # ★核心库（包）
│   ├── __init__.py
│   ├── config.py        # 配置模块（dataclass + .env）
│   ├── database.py      # 数据库连接（SQLAlchemy Engine/Session + Docker引导）
│   ├── db_docker.py     # Docker数据库引导（下载前自动检测/启动timescale容器）
│   ├── models.py        # ORM数据模型（candles/funding_rates/download_state）
│   ├── okx_client.py    # OKX API客户端（限速/重试/线程本地Session/代理池）
│   ├── proxy_pool.py    # IP代理池（每IP平滑限速/健康管理/IP去重探测）
│   ├── dynamic_pool.py  # ★动态IP代理池（自动发现/测IP/应用listeners）
│   ├── downloader/
│   │   ├── __init__.py
│   │   ├── candles.py   # K线下载模块（缺失窗口检测/回溯/批量入库）
│   │   └── funding.py   # 资金费率下载模块
│   └── utils/
│       ├── __init__.py
│       ├── logger.py    # 日志系统（控制台+文件轮转）
│       └── time_utils.py# 时间工具
├── main.py              # ★入口：单币种下载/增量/初始化数据库
├── download_all.py      # ★入口：全历史并行下载（支持IP代理池）
├── sync_continuous.py   # ★入口：连续同步（先全量追赶，再低资源实时同步）
├── sync_daemon.py       # ★入口：常驻实时同步守护（K线+资金费率）
├── tests/               # 单元测试（离线，python -m unittest discover -s tests）
├── pyproject.toml       # 包元数据/依赖（可选 pip install -e .）
├── env.template         # 环境变量模板
├── requirements.txt     # Python依赖
├── runtime/             # 动态池运行时产物（节点缓存/监听配置，已忽略不上传）
└── logs/                # 日志目录（已忽略）
```

> 代码组织：`app/` 为可复用的库代码包，根目录四个入口脚本是面向任务的 CLI，
> 均可直接 `python xxx.py` 运行。单元测试位于 `tests/`（离线，不依赖网络/数据库）。

## 环境要求

- Python 3.9+
- PostgreSQL 12+ （含 TimescaleDB 扩展）

## 安装

```bash
# 1. 安装PostgreSQL和TimescaleDB
#    https://docs.timescale.com/self-hosted/latest/install/

# 2. 克隆/进入项目目录
cd okx_data_downloader

# 3. 创建虚拟环境
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 4. 安装依赖
pip install -r requirements.txt

# 5. 配置环境变量
cp env.template .env      # Windows: copy env.template .env
# 编辑 .env，填入你的数据库和OKX API信息
```

## 配置说明

复制 `env.template` 为 `.env` 后填写：

| 变量 | 说明 |
|------|------|
| `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE` | OKX API密钥（行情接口可留空） |
| `OKX_SANDBOX` | `true`=模拟盘, `false`=实盘 |
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` | PostgreSQL连接信息 |
| `DB_USE_DOCKER` | `auto`=数据库不可达时自动用Docker启动(默认)，`false`=绝不动Docker |
| `DB_CONTAINER_NAME` / `DB_DOCKER_IMAGE` / `DB_DOCKER_VOLUME` | Docker数据库容器名/镜像/数据卷 |
| `LOG_LEVEL` | 日志级别 |
| `DEFAULT_INSTRUMENT` | 默认交易对 |

## Docker 数据库自动启动

运行任何下载/同步脚本前，程序会自动确保数据库可用（`db_docker.py`，挂在 `database.get_engine()` 内，所有入口自动生效）：

1. **数据库已可连接**（`DB_HOST:DB_PORT` 端口可达）→ 直接用现有数据库，不打扰；
2. **数据库不可达** 且本机有 Docker（`DB_USE_DOCKER` 非 `false`）→ 自动检测 Docker，
   创建或启动 `timescale/timescaledb` 容器（容器不存在则自动创建并持久化到命名卷，
   已停止则自动 `docker start`），等待就绪后继续下载；
3. **Docker 也不可用** 或 `DB_USE_DOCKER=false` → 抛出明确错误，提示如何修复。

```bash
# 场景1：本机已有 PostgreSQL，直接连接（默认行为，无需改动）
python download_all.py --dynamic --pool-size 16

# 场景2：本机没有数据库，但有 Docker Desktop —— 自动启动 timescale 容器
python download_all.py --dynamic --pool-size 16

# 场景3：手动管理容器（可选）
docker run -d --name okx-timescaledb --restart unless-stopped \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=okx_data \
  -p 127.0.0.1:5432:5432 -v okx-timescaledb-data:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg16
```

> 说明：只有 `DB_HOST` 为本机地址（localhost/127.0.0.1）时才允许自动启动 Docker；
> 远程数据库不可达时会直接报错。容器首次创建需拉取镜像，建议提前 `docker pull`。

## 使用说明

### 1. 初始化数据库表结构

```bash
python main.py --init-db-only
```

### 2. 下载K线数据

```bash
# 使用默认配置下载（30天，ETH-USDT-SWAP/BTC-USDT-SWAP 等，1m粒度）
python main.py

# 指定交易对和时间粒度
python main.py --inst ETH-USDT-SWAP --bar 4H

# 指定时间范围
python main.py --inst BTC-USDT --bar 1D --start 2024-01-01 --end 2025-12-31

# 只下载K线
python main.py --type candles

# 增量更新最近7天数据
python main.py --update --lookback 7
```

### 3. 全历史并行下载（多合约，推荐）

`download_all.py` 用于**批量下载所有 USDT 永续合约的全历史 K 线**，是功能最完整、速度最快的入口。

核心优化：
- **listTime 下界**：每个合约只回溯到它的实际上线时间（避开新币从 2019 起做无效回溯）
- **合约 × 时间窗并行**：按天切成时间窗，`ThreadPoolExecutor` 并行拉满吞吐
- **窗口级断点续传**：已下载窗口自动跳过，`Ctrl+C` 中断后重跑不重复
- **IP代理池**：每个币绑定一个独立出口IP，每个IP独立限速，吞吐 ≈ IP数×8 req/s（平滑限速消除429突发后实测可达）

```bash
# 全历史下载所有 USDT 永续 1m K线（默认）
python download_all.py

# 自定义并发数与时间粒度
python download_all.py --workers 8 --bar 5m

# 指定部分合约 + 起始时间
python download_all.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP --start 2020-01-01
```

### 3.1 IP代理池：动态模式（推荐，兼容节点/IP变化的VPN）

VPN服务商节点经常变化，**不要写死节点和IP**。使用 `--dynamic` 让程序
在每次下载前自动完成：发现节点 → 逐个测试出口IP → 选独立IP →
生成 Mihomo listeners（每节点一个本地端口）→ 写入 Clash Verge
Merge.yaml → 等待内核重启 → 验证端口 → 构建动态代理池。

```bash
# 动态IP池：自动选独立IP（默认16，本机有33个不同出口IP可--pool-size 32），
# 每IP 4个并发（--workers 自动设为IP数×4）
python download_all.py --dynamic --pool-size 16

# 用缓存复用上次测试结果（TTL 600s，适合频繁增量运行）
python download_all.py --dynamic --pool-ttl 600

# 手动查看动态池运行产物（节点IP缓存 / 生成的listeners配置）
# 位置：runtime/dynamic_pool_cache.json、runtime/mihomo_listeners.yaml
```

首次使用 `--dynamic` 时，程序会把 listeners 写入 Clash Verge 的
`Merge.yaml` 并提示**重启内核**（托盘图标 → 重启内核），随后自动轮询
端口就绪并开始下载。需要 Clash/Mihomo 运行中（external-controller 默认
`127.0.0.1:9097`，混合端口 `127.0.0.1:7890`）。

> 说明：Mihomo `listeners` 让每个端口流量绕过规则直走指定节点，
> 从而在**同一时刻**使用多个不同出口IP。节点/IP变化时，下次运行
> `--dynamic` 会自动重新测试并替换配置。

> 性能实测（2026-08，干净环境）：OKX history-candles 按 IP 限频约
> 20 请求/2秒。旧版令牌桶会"攒令牌→瞬间抽干"造成请求突发，频繁触发
> 429；现已改为平滑限速（严格按间隔发放）。配合 16 个独立IP × 每IP 2
> 线程 × 限速 8/s，实测约 **95+ 页/秒（≈9500 根K线/秒）且几乎无429**，
> 相比默认 8 并发串行提升约 10~15 倍。

### 3.2 IP代理池：静态模式

已有固定多IP代理（如付费住宅/机房代理，每个出口IP不同）时，直接在
`.env` 配置，无需 Clash：

```bash
# .env
OKX_PROXY_URLS=http://127.0.0.1:7891,http://127.0.0.1:7892,http://127.0.0.1:7893
OKX_IP_RATE_LIMIT_PER_SECOND=8

# --workers 建议为 IP 数×4（每IP保持4线程，吃满每IP限速）
# 本机有33个不同出口IP，--pool-size 32 可提速约2倍
python download_all.py --proxy-pool --proxy-verify --workers 16
```

### 4. 连续同步下载（两阶段：先全量追赶，再低资源实时同步）

`sync_continuous.py` 启动后**一直运行**，分两个阶段（日志中用醒目标识明确标注当前阶段）：

**阶段1【全量同步/追赶】**——让数据库数据与OKX API对齐
- 复用 `CandleDownloader.download_range`（缺失窗口检测 + 逐页回溯 + 批量入库），
  把每个合约缺失的数据一次性补齐到当前时刻。下载量可能很大（数据库为空时即全历史下载）。
- **不限制系统资源**：高并发满载追赶（`--workers` 默认代理池下 IP数×4，最高96）。
- 全部合约追平（落后 < `--catchup-lag` 分钟，默认10）后自动进入阶段2。
- 大流量追赶进行中可直接 Ctrl+C 停止，已写入数据保留，下次启动继续。

**阶段2【实时同步】**——低资源后台无感下载
- 每轮只取增量（默认每合约最多10页），有界队列 + 独立批量写库线程，幂等入库；
- **限制系统资源**：默认并发 `--rt-workers 4`，追平时按K线周期对齐休眠
  （1m粒度每轮仅约2秒下载 + 其余时间空闲，实测内存~57MB、CPU近乎0）；
- 每30分钟重同步合约列表，自动纳入新上市币种。

```bash
# 默认：一直运行（阶段1追赶 -> 阶段2实时同步）
python sync_continuous.py

# 指定时长（阶段2运行 N 小时后退出）
python sync_continuous.py --hours 12

# 数据已最新时跳过追赶，直接实时同步
python sync_continuous.py --skip-catchup

# 配合IP代理池（两阶段都提速，阶段1自动高并发）
python sync_continuous.py --dynamic --pool-size 16

# 指定部分合约
python sync_continuous.py --insts BTC-USDT-SWAP,ETH-USDT-SWAP
```

> 两个阶段都遵守 OKX 每IP平滑限速（`proxy_pool`，默认8 req/s/IP），这是 API
> 约束而非本机资源限制，目的是避免429风暴。断点续传由 `sync_state` 水位线表
> 保证，重启后从断点继续。

### 5. 下载资金费率

```bash
# 只下载资金费率（仅合约产品）
python main.py --type funding --inst ETH-USDT-SWAP
```

### 6. 编程式使用

```python
from app.config import Config
from app.okx_client import OKXClient
from app.downloader.candles import CandleDownloader
from app.database import init_db, dispose_engine
from datetime import datetime, timedelta

init_db()
config = Config()
client = OKXClient()
dl = CandleDownloader(client, config)

dl.download_range(
    inst_id="ETH-USDT-SWAP",
    bar="4H",
    start=datetime.utcnow() - timedelta(days=30),
    end=datetime.utcnow(),
)
dispose_engine()
```

### 7. 单元测试

```bash
# 离线测试（不依赖网络/数据库）
python -m unittest discover -s tests -v
```

## 日志规范

所有脚本统一日志规则（`app/utils/logger.py`）。

**格式**：`{时间} {级别} | {组件} | {消息}`

```
2026-08-15 14:24:31 INFO  | sync_continuous | [阶段2 实时同步] 第1轮 | 抓到 3 根 | 失败 0 | 耗时 2.9s
14:24:31 INFO  | downloader.candles | [进度] AVAX-USDT-SWAP | 1m | 回溯至 2023-04-25 05:20 | 页 400
```

- 组件名 = 模块名去掉 `app.` 前缀（`app.downloader.candles` → `downloader.candles`）；入口脚本用脚本名（`main`/`download_all`/`sync_continuous`/`sync_daemon`）
- 控制台简洁（无日期），日志文件完整（带日期毫秒）

**级别规则**

| 级别 | 用途 |
|------|------|
| `DEBUG` | 底层细节（单请求/响应、SQL、调试信息） |
| `INFO` | 正常进度（任务开始/完成、阶段切换、轮次汇总、定期进度） |
| `WARNING` | 可恢复的瞬时问题（单次重试、换代理、单合约失败、单次429） |
| `ERROR` | 持久失败、异常导致功能中断 |

**文件规则**

- 每个入口脚本独立日志文件：`logs/{脚本名}_{日期}.log`，避免多进程互相穿插
- 单文件最大 10MB，轮转保留 5 个备份；UTF-8 编码
- 进度日志按时间间隔输出（大合约回溯每 30 秒一条，不刷屏）
- 第三方库降噪：`urllib3=ERROR`、`sqlalchemy=WARNING`

## 数据库表结构

### `candles` (K线)

| 字段 | 类型 | 说明 |
|------|------|------|
| `inst_id` | VARCHAR(50) | 产品ID |
| `bar` | VARCHAR(10) | 时间粒度 |
| `ts` | TIMESTAMPTZ | 时间戳（PK） |
| `o` / `h` / `l` / `c` | NUMERIC | 开盘/最高/最低/收盘 |
| `vol` | NUMERIC | 成交量 |
| `vol_ccy` | NUMERIC | 计价量 |
| `confirm` | VARCHAR | K线状态 |

### `funding_rates` (资金费率)

| 字段 | 类型 | 说明 |
|------|------|------|
| `inst_id` | VARCHAR(50) | 合约ID |
| `ts` | TIMESTAMPTZ | 时间戳（PK） |
| `funding_rate` | NUMERIC | 资金费率 |
| `realized_rate` | NUMERIC | 已实现费率 |
| `funding_time` | TIMESTAMPTZ | 结算时间 |

### `sync_state` (同步水位线，sync_continuous 自动维护)

| 字段 | 类型 | 说明 |
|------|------|------|
| `inst_id` | VARCHAR(50) | 合约ID（PK） |
| `bar` | VARCHAR(10) | K线粒度（PK） |
| `latest_ts` | TIMESTAMPTZ | 该合约已同步到的最新K线时间 |
| `updated_at` | TIMESTAMPTZ | 更新时间 |

`candles` 与 `funding_rates` 均会通过 `init_db()` 自动转换为 **TimescaleDB hypertable**，按 `ts` 分区，提升时序查询性能；`sync_state` 为小表（每合约一行），由 `sync_continuous.py` 自动创建维护。

## TimescaleDB 优势

- 🚀 时间相关查询性能提升 10-100x
- 📊 内置连续聚合（continuous aggregates）可用于构建指标
- 💾 自动数据保留策略（retention policy）节省存储
- 🔍 原生 `time_bucket()` 等时序聚合函数

## 定时任务（可选）

可通过 cron / Task Scheduler 定时执行增量更新：

```bash
# 每小时增量更新最近1小时K线
python main.py --update --lookback 1 --type candles

# 每天增量更新最近1天资金费率
python main.py --update --lookback 1 --type funding
```

## 常见问题

**Q: 需要付费API key吗？**
A: 下载公开行情数据（K线/资金费率）不需要API鉴权，`API_KEY` 等可留空。

**Q: TimescaleDB必须安装吗？**
A: 不是强制要求。代码会尝试创建hypertable，若TimescaleDB不可用则回退为普通表。

**Q: 支持哪些时间粒度？**
A: OKX支持 `1m/3m/5m/15m/30m/1H/2H/4H/12H/1D/1W/1M` 等13种。

## 许可证

MIT License
