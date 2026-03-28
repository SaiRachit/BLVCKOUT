import json
import logging
from datetime import datetime
from datetime import timezone
from enum import StrEnum
from typing import Any

from services.common.app.streaming import get_stream_publisher


class EventType(StrEnum):
    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    CACHE_HIT = "CACHE_HIT"
    CACHE_MISS = "CACHE_MISS"
    DB_QUERY = "DB_QUERY"
    ERROR = "ERROR"
    RESPONSE_SENT = "RESPONSE_SENT"


def utc_timestamp_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": utc_timestamp_ms(),
            "service": getattr(record, "service", "unknown"),
            "trace_id": getattr(record, "trace_id", None),
            "event_type": getattr(record, "event_type", None),
            "message": record.getMessage(),
            "level": record.levelname,
            "path": getattr(record, "path", None),
            "method": getattr(record, "method", None),
            "status_code": getattr(record, "status_code", None),
            "latency": getattr(record, "latency", None),
            "error": getattr(record, "error", None),
            "metrics": getattr(record, "metrics", None),
            "extra": getattr(record, "extra", None),
        }
        return json.dumps(payload, ensure_ascii=True)


def configure_logger(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


def log_event(
    logger: logging.Logger,
    *,
    service: str,
    trace_id: str,
    event_type: EventType,
    message: str,
    path: str | None = None,
    method: str | None = None,
    status_code: int | None = None,
    latency: float | None = None,
    error: str | None = None,
    metrics: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
    level: int = logging.INFO,
) -> None:
    payload = {
        "timestamp": utc_timestamp_ms(),
        "service": service,
        "trace_id": trace_id,
        "event_type": event_type.value,
        "path": path,
        "method": method,
        "status_code": status_code,
        "latency": latency,
        "error": error,
        "metrics": metrics,
        "extra": extra,
    }
    reserved_keys = set(logging.makeLogRecord({}).__dict__.keys()) | {"message", "asctime"}
    logger.log(
        level,
        message,
        extra={
            key: value for key, value in payload.items() if key not in reserved_keys
        },
    )
    get_stream_publisher(logger).publish_log(payload)
