"""命令行入口包

根目录的 backfill.py / sync_continuous.py / sync_realtime.py /
latency_probe.py / quality_report.py 为极薄 wrapper，统一转发到 cli.*。
"""

__all__ = ["backfill", "sync_continuous", "sync_realtime",
           "latency_probe", "quality_report"]
