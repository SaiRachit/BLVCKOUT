import threading

import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
from services.common.app.contracts import DataResponse
from services.common.app.contracts import TogglePayload
from services.common.app.faults import FaultState
from services.common.app.faults import ensure_upstream_success
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import TRACE_ID_HEADER
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan

SERVICE_NAME = get_env("SERVICE_NAME", "auth-service")
CACHE_SERVICE_URL = get_env("CACHE_SERVICE_URL", "http://localhost:8001")

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)
_auth_error_enabled = False
_lock = threading.Lock()


def _auth_error_state() -> bool:
    with _lock:
        return _auth_error_enabled


@app.post("/faults/auth-error")
async def set_auth_error(payload: TogglePayload) -> dict[str, bool]:
    global _auth_error_enabled
    with _lock:
        _auth_error_enabled = payload.enabled
    return {"auth_error_enabled": _auth_error_enabled}


@app.get("/auth/validate/{key}")
async def validate_access(key: str, request: Request) -> dict[str, str | bool]:
    trace_id = request.state.trace_id
    if _auth_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="authentication failed",
            path=f"/auth/validate/{key}",
            method="GET",
            error="auth error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="authentication unavailable")
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.REQUEST_RECEIVED,
        message="auth validation completed",
        path=f"/auth/validate/{key}",
        method="GET",
    )
    return {"service": SERVICE_NAME, "key": key, "authorized": True}


@app.get("/auth/items/{key}", response_model=DataResponse)
async def authorize_and_fetch(key: str, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    if _auth_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="authorization failed",
            path=f"/auth/items/{key}",
            method="GET",
            error="auth error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="authentication unavailable")
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(
                f"{CACHE_SERVICE_URL}/cache/{key}",
                headers={TRACE_ID_HEADER: trace_id},
            )
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="cache request failed from auth",
            path=f"/auth/items/{key}",
            method="GET",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="cache service unavailable") from exc

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"{key} not found")
    ensure_upstream_success(response.status_code, "cache service unavailable")
    payload = response.json()
    return DataResponse(
        key=payload["key"],
        value=payload["value"],
        source=payload["source"],
        served_by=SERVICE_NAME,
        cache_hit=payload.get("cache_hit"),
    )
