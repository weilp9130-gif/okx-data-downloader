"""API 层 Pydantic 模型（请求体/响应体）"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_type: str
    params: Dict[str, Any]


class TaskBatchCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_type: str
    params_list: List[Dict[str, Any]] = Field(min_length=1, max_length=500)


class TaskStopRequest(BaseModel):
    pass


class AssetRefreshRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scope: str = "all"      # all / inst
    inst_id: Optional[str] = None
    mode: str = "incremental"  # incremental / full


class QualityCheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inst_id: str
    bar: str = "1D"
    cross_source: bool = False


class LatencyProbeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    insts: List[str] = Field(min_length=1)
    channels: List[str] = Field(min_length=1)
    duration: int = Field(default=60, ge=1, le=86400)


class TaskResponse(BaseModel):
    id: str
    group_id: Optional[str] = None
    task_no: str
    task_type: str
    params: Dict[str, Any]
    status: str
    required_capability: Optional[str] = None
    rate_group: Optional[str] = None
    attempt_no: int
    retry_count: int
    max_retry: int
    progress: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: Any = None
    started_at: Optional[Any] = None
    finished_at: Optional[Any] = None
