import threading

import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
from services.common.app.contracts import DataResponse
from services.common.app.contracts import TogglePayload
from services.common.app.contracts import ValuePayload
from services.common.app.faults import FaultState
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import TRACE_ID_HEADER
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan

SERVICE_NAME = get_env("SERVICE_NAME", "database-service")
SERVICE_PORT = int(get_env("SERVICE_PORT", "8002"))

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)
_database_error_enabled = False
_lock = threading.Lock()

DB: dict[str, str] = {
    "user:123": "premium",
    "user:456": "standard",
    "feature:search": "enabled",
}


def _database_error_state() -> bool:
    with _lock:
        return _database_error_enabled


@app.post("/faults/database-error")
async def set_database_error(payload: TogglePayload) -> dict[str, bool]:
    global _database_error_enabled
    with _lock:
        _database_error_enabled = payload.enabled
    return {"database_error_enabled": _database_error_enabled}


@app.get("/db/{key}", response_model=DataResponse)
async def get_value(key: str, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    if _database_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="database request failed",
            path=f"/db/{key}",
            method="GET",
            error="database error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="database unavailable")
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.DB_QUERY,
        message="database query executed",
        path=f"/db/{key}",
        method="GET",
    )
    if key not in DB:
        raise HTTPException(status_code=404, detail=f"{key} not found")
    return DataResponse(key=key, value=DB[key], source="database")


@app.put("/db/{key}", response_model=DataResponse)
async def put_value(key: str, payload: ValuePayload, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    if _database_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="database write failed",
            path=f"/db/{key}",
            method="PUT",
            error="database error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="database unavailable")
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.DB_QUERY,
        message="database write executed",
        path=f"/db/{key}",
        method="PUT",
    )
    DB[key] = payload.value
    return DataResponse(key=key, value=payload.value, source="database")
