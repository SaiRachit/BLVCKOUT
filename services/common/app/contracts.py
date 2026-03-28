from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator


class ValuePayload(BaseModel):
    value: str


class CacheWritePayload(BaseModel):
    value: str
    source: str = "api"


class FaultLatencyPayload(BaseModel):
    extra_latency_ms: int = Field(ge=0, le=60000)


class FaultMemoryLeakPayload(BaseModel):
    enabled: bool
    chunk_size_kb: int = Field(default=256, ge=1, le=4096)


class TogglePayload(BaseModel):
    enabled: bool


class ServiceHealth(BaseModel):
    service: str
    status: str
    faults: dict[str, Any]


class MetricsSnapshot(BaseModel):
    service: str
    trace_id: str | None = None
    request_count: int
    error_count: int
    error_rate: float
    avg_latency_ms: float
    p95_latency_ms: float
    uptime_seconds: int


class DataResponse(BaseModel):
    key: str
    value: str
    source: str
    served_by: Optional[str] = None
    cache_hit: Optional[bool] = None


class ExternalIngestPayload(BaseModel):
    service: str
    event_type: str
    latency: float = Field(ge=0)
    timestamp: str
    trace_id: str
    metric_name: str | None = None
    metric_value: float | None = None
    error_rate: float | None = Field(default=None, ge=0, le=1)
    status: str | None = None
    message: str | None = None
    source: str | None = None

    @field_validator("service", "event_type", "trace_id")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be empty")
        return normalized

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be empty")
        try:
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("must be a valid ISO 8601 timestamp") from exc
        return normalized


class IngestResponse(BaseModel):
    status: str
    topic: str
    service: str
    trace_id: str
    pipeline: str


class IngestTestResponse(BaseModel):
    status: str
    pipeline: str
    trace_id: str
    normalized_detected: bool
    rca_status: str
