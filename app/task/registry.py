"""任务注册表：task_type → TaskSpec（校验 + argv 构造 + capability + rate_group + on_success）

- params 必须通过 TaskSpec 校验（extra="forbid"、类型/枚举/正则）才构造 argv；
  恶意/未知字段 422。
- argv 只由 TaskSpec 构造（shell=False），杜绝注入。
- required_capability 在 submit 时由注册表写入 jobs 行；Worker 认领按 capability 过滤。
"""

import sys
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Type

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..config.config import Config
from ..utils.logger import get_logger
from ..utils.time_utils import parse_date

logger = get_logger(__name__)


class TaskSpec:
    """单个任务类型的规范定义"""

    def __init__(
        self,
        task_type: str,
        params_model: Type[BaseModel],
        capability: str,
        rate_group: Optional[str],
        build_argv: Callable[[dict, str], List[str]],
        on_success: Optional[Callable] = None,
    ):
        self.task_type = task_type
        self.params_model = params_model
        self.capability = capability
        self.rate_group = rate_group
        self.build_argv = build_argv
        self.on_success = on_success

    def validate(self, params: dict) -> dict:
        """校验 params，返回规范化 dict（非法抛 pydantic.ValidationError → 422）"""
        return self.params_model.model_validate(params).model_dump()

    def command_argv(self, params: dict, progress_file: str) -> List[str]:
        """构造完整 argv（含解释器），供 Popen(shell=False) 使用"""
        return self.build_argv(params, progress_file)


def _base_argv(module: str) -> List[str]:
    return [sys.executable, "-m", module]


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


# ====================================================================
# 参数模型（extra="forbid"，防注入）
# ====================================================================

class _TimeParams(BaseModel):
    start: str
    end: str

    @field_validator("start", "end")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        parse_date(v)
        return v


class KlineParams(_TimeParams):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    bars: List[str] = Field(min_length=1)
    strategy: str = "patch"
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v

    @field_validator("bars")
    @classmethod
    def _bars_valid(cls, v: List[str]) -> List[str]:
        allowed = set(Config().download.kline_bars)
        for b in v:
            if b not in allowed:
                raise ValueError(f"不支持的 bar: {b}（可选: {sorted(allowed)}）")
        return v

    @field_validator("strategy")
    @classmethod
    def _strategy_valid(cls, v: str) -> str:
        if v not in ("patch", "full", "incremental"):
            raise ValueError("strategy 必须为 patch/full/incremental")
        return v


class TradesParams(_TimeParams):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    max_pages: int = Field(default=10, ge=1, le=100)
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v


class FundingParams(_TimeParams):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v


class PriceParams(_TimeParams):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    bar: str = Field(min_length=1, max_length=10)
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v


class OpenInterestParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v


class InstrumentsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inst_type: str = "SWAP"

    @field_validator("inst_type")
    @classmethod
    def _inst_type_valid(cls, v: str) -> str:
        if v != "SWAP":
            raise ValueError("首版仅支持 SWAP")
        return v


class LatencyProbeParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    insts: List[str] = Field(min_length=1)
    channels: List[str] = Field(min_length=1)
    duration: int = Field(ge=1, le=86400)
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("insts")
    @classmethod
    def _insts_shape(cls, v: List[str]) -> List[str]:
        for i in v:
            if not i or any(ch.isspace() for ch in i):
                raise ValueError(f"inst 非法: {i!r}")
        return v

    @field_validator("channels")
    @classmethod
    def _channels_valid(cls, v: List[str]) -> List[str]:
        from ..latency.ws_probe import ALLOWED_CHANNELS
        for c in v:
            if c not in ALLOWED_CHANNELS:
                raise ValueError(
                    f"不支持的频道: {c}（可选: {sorted(ALLOWED_CHANNELS)}）"
                )
        return v


class QualityCheckParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inst: str = Field(min_length=1, max_length=50)
    bar: str = Field(default="1D", max_length=10)
    cross_source: bool = False
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("inst")
    @classmethod
    def _inst_shape(cls, v: str) -> str:
        if not v or any(ch.isspace() for ch in v):
            raise ValueError("inst 非法")
        return v


class AssetRefreshParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scope: str = "all"
    mode: str = "incremental"
    inst_id: Optional[str] = None
    max_retry: int = Field(default=0, ge=0, le=3)
    priority: int = Field(default=0, ge=0, le=10)

    @field_validator("scope")
    @classmethod
    def _scope_valid(cls, v: str) -> str:
        if v not in ("all", "inst"):
            raise ValueError("scope 必须为 all/inst")
        return v

    @field_validator("mode")
    @classmethod
    def _mode_valid(cls, v: str) -> str:
        if v not in ("incremental", "full"):
            raise ValueError("mode 必须为 incremental/full")
        return v

    @model_validator(mode="after")
    def _inst_required_for_scope(self):
        if self.scope == "inst" and not self.inst_id:
            raise ValueError("scope=inst 时必须提供 inst_id")
        return self


# ====================================================================
# argv 构造器
# ====================================================================

def _build_kline(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.backfill")
    start, end = params["start"], params["end"]
    if params["strategy"] == "incremental":
        end = _fmt(datetime.now(timezone.utc))
        start = _fmt(datetime.now(timezone.utc) - timedelta(days=1))
    argv += [
        "--type", "candles",
        "--inst", params["inst"],
        "--bar", ",".join(params["bars"]),
        "--start", start,
        "--end", end,
    ]
    if params["strategy"] == "full":
        argv.append("--overwrite")
    argv += ["--progress-file", progress_file]
    return argv


def _build_trades(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.backfill")
    argv += [
        "--type", "trades",
        "--inst", params["inst"],
        "--start", params["start"],
        "--end", params["end"],
        "--max-pages", str(params["max_pages"]),
        "--progress-file", progress_file,
    ]
    return argv


def _build_funding(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.backfill")
    argv += [
        "--type", "funding",
        "--inst", params["inst"],
        "--start", params["start"],
        "--end", params["end"],
        "--progress-file", progress_file,
    ]
    return argv


def _build_price(task_type: str):
    def _build(params: dict, progress_file: str) -> List[str]:
        argv = _base_argv("cli.backfill")
        data_type = "mark" if task_type == "MARK_PRICE" else "index"
        argv += [
            "--type", data_type,
            "--inst", params["inst"],
            "--bar", params["bar"],
            "--start", params["start"],
            "--end", params["end"],
            "--progress-file", progress_file,
        ]
        return argv
    return _build


def _build_open_interest(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.backfill")
    argv += [
        "--type", "oi",
        "--inst", params["inst"],
        "--progress-file", progress_file,
    ]
    return argv


def _build_instruments(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.backfill")
    argv += [
        "--type", "instruments",
        "--inst-type", params["inst_type"],
        "--progress-file", progress_file,
    ]
    return argv


def _build_latency_probe(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.latency_probe")
    argv += [
        "--insts", ",".join(params["insts"]),
        "--channels", ",".join(params["channels"]),
        "--duration", str(params["duration"]),
        "--summary-interval", "60",
        "--progress-file", progress_file,
    ]
    return argv


def _build_quality_check(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.quality_report")
    output_json = progress_file[:-len(".jsonl")] + ".json" if progress_file.endswith(".jsonl") else progress_file + ".json"
    argv += [
        "--type", "all",
        "--inst", params["inst"],
        "--bar", params["bar"],
        "--output", output_json,
    ]
    if params.get("cross_source"):
        argv.append("--cross-source")
    return argv


def _build_asset_refresh(params: dict, progress_file: str) -> List[str]:
    argv = _base_argv("cli.asset_refresh")
    argv += [
        "--scope", params["scope"],
        "--mode", params["mode"],
        "--progress-file", progress_file,
    ]
    if params.get("inst_id"):
        argv += ["--inst", params["inst_id"]]
    return argv


# ====================================================================
# on_success 钩子
# ====================================================================

def _on_quality_check_success(store, job: dict, attempt: dict) -> None:
    """QUALITY_CHECK 完成后解析报告 JSON → 写 data_asset_state"""
    from ..services.quality_score import apply_quality_report

    progress_path = attempt.get("progress_path") or ""
    output_json = (
        progress_path[:-len(".jsonl")] + ".json"
        if progress_path.endswith(".jsonl")
        else progress_path + ".json"
    )
    apply_quality_report(output_json, store=store, job=job)


# ====================================================================
# 注册表
# ====================================================================

REGISTRY: Dict[str, TaskSpec] = {}


def _register(spec: TaskSpec) -> None:
    REGISTRY[spec.task_type] = spec


_register(TaskSpec(
    "KLINE", KlineParams, capability="download", rate_group="okx_market",
    build_argv=_build_kline,
))
_register(TaskSpec(
    "TRADES", TradesParams, capability="download", rate_group="okx_market",
    build_argv=_build_trades,
))
_register(TaskSpec(
    "FUNDING_RATE", FundingParams, capability="download", rate_group="okx_market",
    build_argv=_build_funding,
))
_register(TaskSpec(
    "MARK_PRICE", PriceParams, capability="download", rate_group="okx_market",
    build_argv=_build_price("MARK_PRICE"),
))
_register(TaskSpec(
    "INDEX_PRICE", PriceParams, capability="download", rate_group="okx_market",
    build_argv=_build_price("INDEX_PRICE"),
))
_register(TaskSpec(
    "OPEN_INTEREST", OpenInterestParams, capability="download", rate_group="okx_market",
    build_argv=_build_open_interest,
))
_register(TaskSpec(
    "INSTRUMENTS", InstrumentsParams, capability="download", rate_group="okx_market",
    build_argv=_build_instruments,
))
_register(TaskSpec(
    "LATENCY_PROBE", LatencyProbeParams, capability="latency", rate_group="okx_ws",
    build_argv=_build_latency_probe,
))
_register(TaskSpec(
    "QUALITY_CHECK", QualityCheckParams, capability="download", rate_group="okx_market",
    build_argv=_build_quality_check,
    on_success=_on_quality_check_success,
))
_register(TaskSpec(
    "ASSET_REFRESH", AssetRefreshParams, capability="download", rate_group=None,
    build_argv=_build_asset_refresh,
))


def get_spec(task_type: str) -> Optional[TaskSpec]:
    return REGISTRY.get(task_type)


def validate_params(task_type: str, params: dict) -> dict:
    """校验并规范化 params（非法抛 pydantic.ValidationError）"""
    spec = get_spec(task_type)
    if spec is None:
        raise ValueError(f"未知任务类型: {task_type}")
    return spec.validate(params)
