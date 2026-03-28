import threading

import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
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

SERVICE_NAME = get_env("SERVICE_NAME", "payment-service")
NOTIFICATION_SERVICE_URL = get_env("NOTIFICATION_SERVICE_URL", "http://localhost:8009")
DATABASE_SERVICE_URL = get_env("DATABASE_SERVICE_URL", "http://localhost:8002")

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)
_payment_error_enabled = False
_lock = threading.Lock()


def _payment_error_state() -> bool:
    with _lock:
        return _payment_error_enabled


@app.post("/faults/payment-error")
async def set_payment_error(payload: TogglePayload) -> dict[str, bool]:
    global _payment_error_enabled
    with _lock:
        _payment_error_enabled = payload.enabled
    return {"payment_error_enabled": _payment_error_enabled}


@app.get("/payments/process/{key}")
async def process_payment(key: str, request: Request) -> dict[str, str | bool]:
    return await purchase_payment(key, request)


@app.get("/payments/purchase/{key}")
async def purchase_payment(key: str, request: Request) -> dict[str, str | bool]:
    trace_id = request.state.trace_id
    if _payment_error_state():
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="payment processing failed",
            path=f"/payments/purchase/{key}",
            method="GET",
            error="payment error fault enabled",
            level=40,
        )
        raise HTTPException(status_code=503, detail="payment service error simulated")

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.put(
                f"{DATABASE_SERVICE_URL}/db/purchase:{key}",
                json={"value": "completed"},
                headers={TRACE_ID_HEADER: trace_id},
            )
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="database request failed from payment",
            path=f"/payments/purchase/{key}",
            method="GET",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="database service unavailable") from exc

    ensure_upstream_success(response.status_code, "database service unavailable")
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.RESPONSE_SENT,
        message="payment processed",
        path=f"/payments/purchase/{key}",
        method="GET",
        status_code=200,
    )
    return {"service": SERVICE_NAME, "key": key, "payment_processed": True, "transaction_state": "persisted"}
