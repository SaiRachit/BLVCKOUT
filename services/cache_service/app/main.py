import threading

import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
from services.common.app.contracts import CacheWritePayload
from services.common.app.contracts import DataResponse
from services.common.app.contracts import TogglePayload
from services.common.app.faults import FaultState
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import TRACE_ID_HEADER
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan

SERVICE_NAME = get_env("SERVICE_NAME", "cache-service")
DATABASE_SERVICE_URL = get_env("DATABASE_SERVICE_URL", "http://localhost:8002")

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)

CACHE: dict[str, str] = {}
_cache_error_enabled = False
_lock = threading.Lock()


def _cache_error_state() -> bool:
    with _lock:
        return _cache_error_enabled


@app.post("/faults/cache-error")
async def set_cache_error(payload: TogglePayload) -> dict[str, bool]:
    global _cache_error_enabled
    with _lock:
        _cache_error_enabled = payload.enabled
    return {"cache_error_enabled": _cache_error_enabled}


@app.get("/cache/{key}", response_model=DataResponse)
async def get_cached_value(key: str, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    if _cache_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="cache access failed",
            path=f"/cache/{key}",
            method="GET",
            error="cache error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="cache service unavailable")
    if key in CACHE:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.CACHE_HIT,
            message="cache hit",
            path=f"/cache/{key}",
            method="GET",
        )
        return DataResponse(key=key, value=CACHE[key], source="cache", cache_hit=True)

    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.CACHE_MISS,
        message="cache miss",
        path=f"/cache/{key}",
        method="GET",
    )

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"{DATABASE_SERVICE_URL}/db/{key}", headers={TRACE_ID_HEADER: trace_id})
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="database request failed",
            path=f"/cache/{key}",
            method="GET",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="database service unavailable") from exc

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"{key} not found")
    if response.status_code >= 500:
        raise HTTPException(status_code=502, detail="database service unavailable")

    payload = response.json()
    CACHE[key] = payload["value"]
    return DataResponse(key=key, value=payload["value"], source="database", cache_hit=False)


@app.put("/cache/{key}", response_model=DataResponse)
async def put_cached_value(key: str, payload: CacheWritePayload) -> DataResponse:
    if _cache_error_state():
        raise HTTPException(status_code=503, detail="cache service unavailable")
    CACHE[key] = payload.value
    return DataResponse(key=key, value=payload.value, source=payload.source, cache_hit=True)


@app.post("/cache/clear")
async def clear_cache() -> dict[str, str]:
    CACHE.clear()
    return {"status": "cleared"}


@app.post("/internal/clear")
async def internal_clear_cache() -> dict[str, str | int]:
    cleared_entries = len(CACHE)
    CACHE.clear()
    return {"status": "cleared", "cleared_entries": cleared_entries}
