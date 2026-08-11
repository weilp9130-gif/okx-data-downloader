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

## 项目结构

```
okx_data_downloader
├── main.py           # 程序入口（命令行CLI）
├── config.py         # 配置模块（dataclass + .env）
├── database.py       # 数据库连接（SQLAlchemy Engine/Session）
├── models.py         # ORM数据模型
├── okx_client.py     # OKX API客户端
├── downloader/
│   ├── __init__.py
│   ├── candles.py    # K线下载模块
│   └── funding.py    # 资金费率下载模块
├── utils/
│   ├── __init__.py
│   ├── logger.py     # 日志系统
│   └── time_utils.py # 时间工具
├── env.template      # 环境变量模板
├── requirements.txt  # Python依赖
├── run_example.py    # 编程式使用示例
└── logs/             # 日志目录
```

## 环境要求

- Python 3.8+
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
| `LOG_LEVEL` | 日志级别 |
| `DEFAULT_INSTRUMENT` | 默认交易对 |

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

### 3. 下载资金费率

```bash
# 只下载资金费率（仅合约产品）
python main.py --type funding --inst ETH-USDT-SWAP
```

### 4. 编程式使用

参考 `run_example.py`：

```python
from okx_client import OKXClient
from downloader.candles import CandleDownloader
from datetime import datetime, timedelta

config = Config()
client = OKXClient()
dl = CandleDownloader(client, config)

dl.download_range(
    inst_id="ETH-USDT-SWAP",
    bar="4H",
    start=datetime.utcnow() - timedelta(days=30),
    end=datetime.utcnow(),
)
```

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

两张表均会通过 `init_db()` 自动转换为 **TimescaleDB hypertable**，按 `ts` 分区，提升时序查询性能。

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
