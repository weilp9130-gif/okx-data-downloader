"""OKX数据下载器单元测试

离线测试，不依赖网络与数据库。测试按功能分目录，与 app/ 子包对应：
- test_imports: 校验所有模块可导入
- test_package_layout: 目录布局守卫
- downloader/ realtime/ aggregation/ quality/ latency/ client/ proxy/ config/ cli/ utils/

运行：
    python -m unittest discover -s tests -t . -v
"""
