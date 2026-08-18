"""OKX数据下载器核心包

按功能分层：
- downloader/: 历史数据下载（K线、资金费率、交易、持仓量等）
- realtime/: WebSocket 实时采集
- aggregation/: 数据聚合
- quality/: 质量检测（validator + conflict）
- latency/: 延迟探针（独立于 quality）
- client/: OKX REST API 客户端
- db/: 数据库连接、ORM 模型与同步状态
- proxy/: 静态/动态代理池
- config/: 应用配置与下载范围配置
- utils/: 通用工具

命令行入口统一位于 cli/ 包，根目录的同名文件为薄壳 wrapper。
"""
