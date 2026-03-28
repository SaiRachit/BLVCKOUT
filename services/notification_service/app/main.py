import threading
import time

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
from services.common.app.contracts import FaultLatencyPayload
from services.common.app.contracts import TogglePayload
from services.common.app.faults import FaultState
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan

SERVICE_NAME = get_env("SERVICE_NAME", "notification-service")

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)
_notification_delay_ms = 0
_drop_notifications_enabled = False
_lock = threading.Lock()


def _current_delay_ms() -> int:
    with _lock:
        return _notification_delay_ms


def _drop_notifications_state() -> bool:
    with _lock:
        return _drop_notifications_enabled


@app.post("/faults/notification-delay")
async def set_notification_delay(payload: FaultLatencyPayload) -> dict[str, int]:
    global _notification_delay_ms
    with _lock:
        _notification_delay_ms = payload.extra_latency_ms
    return {"notification_delay_ms": _notification_delay_ms}


@app.post("/faults/notification-drop")
async def set_notification_drop(payload: TogglePayload) -> dict[str, bool]:
    global _drop_notifications_enabled
    with _lock:
        _drop_notifications_enabled = payload.enabled
    return {"notification_drop_enabled": _drop_notifications_enabled}


@app.post("/notifications/send/{key}")
async def send_notification(key: str, request: Request) -> dict[str, str | int]:
    trace_id = request.state.trace_id
    delay_ms = _current_delay_ms()
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    if _drop_notifications_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="notification dropped",
            path=f"/notifications/send/{key}",
            method="POST",
            error="notification drop fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="notification delivery failed")
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.RESPONSE_SENT,
        message="notification delivered",
        path=f"/notifications/send/{key}",
        method="POST",
        status_code=200,
        latency=float(delay_ms),
    )
    return {"service": SERVICE_NAME, "key": key, "notification_delay_ms": delay_ms, "delivered": True}
