import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse

from services.common.app.contracts import FaultLatencyPayload
from services.common.app.contracts import FaultMemoryLeakPayload
from services.common.app.contracts import MetricsSnapshot
from services.common.app.contracts import ServiceHealth
from services.common.app.faults import FaultState
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.metrics import render_prometheus_metrics

TRACE_ID_HEADER = "x-trace-id"
QUIET_PATHS = {
    "/",
    "/dashboard",
    "/health",
    "/metrics",
    "/rca/latest",
    "/rca/timeline",
    "/incidents",
    "/debug/latency",
    "/normalized-events",
    "/trace/view",
    "/export/prometheus",
    "/export/loki",
    "/export/jaeger",
}


def attach_common_routes(app: FastAPI, service_name: str, faults: FaultState, metrics: MetricsRegistry) -> None:
    logger = configure_logger(service_name)

    @app.middleware("http")
    async def instrumentation(request: Request, call_next):
        trace_id = request.headers.get(TRACE_ID_HEADER, uuid.uuid4().hex)
        request.state.trace_id = trace_id
        start = time.perf_counter()
        success = False
        response = None
        quiet_methods = {"GET", "HEAD"}
        quiet_request = (
            service_name == "api-service"
            and request.method in quiet_methods
            and request.url.path in QUIET_PATHS
        )
        faults.apply_latency()
        faults.tick_memory_leak()
        if not quiet_request:
            log_event(
                logger,
                service=service_name,
                trace_id=trace_id,
                event_type=EventType.REQUEST_RECEIVED,
                message="request received",
                path=request.url.path,
                method=request.method,
            )
        try:
            response = await call_next(request)
            success = response.status_code < 400
            response.headers[TRACE_ID_HEADER] = trace_id
            return response
        except Exception as exc:
            latency_ms = round((time.perf_counter() - start) * 1000, 2)
            metrics.record(latency_ms, success=False)
            metric_snapshot = metrics.emit_snapshot(trace_id=trace_id)
            log_event(
                logger,
                service=service_name,
                trace_id=trace_id,
                event_type=EventType.ERROR,
                message="request failed",
                path=request.url.path,
                method=request.method,
                status_code=500,
                latency=latency_ms,
                error=str(exc),
                metrics=metric_snapshot,
                level=40,
            )
            error_response = JSONResponse(status_code=500, content={"detail": "internal server error"})
            error_response.headers[TRACE_ID_HEADER] = trace_id
            return error_response
        finally:
            if response is not None:
                latency_ms = round((time.perf_counter() - start) * 1000, 2)
                metrics.record(latency_ms, success=success)
                metric_snapshot = metrics.snapshot(trace_id=trace_id) if quiet_request else metrics.emit_snapshot(trace_id=trace_id)
                if response.status_code >= 400:
                    log_event(
                        logger,
                        service=service_name,
                        trace_id=trace_id,
                        event_type=EventType.ERROR,
                        message="request returned error response",
                        path=request.url.path,
                        method=request.method,
                        status_code=response.status_code,
                        latency=latency_ms,
                        metrics=metric_snapshot,
                        level=40,
                    )
                if not quiet_request:
                    log_event(
                        logger,
                        service=service_name,
                        trace_id=trace_id,
                        event_type=EventType.RESPONSE_SENT,
                        message="response sent",
                        path=request.url.path,
                        method=request.method,
                        status_code=response.status_code,
                        latency=latency_ms,
                        metrics=metric_snapshot,
                    )

    @app.get("/health", response_model=ServiceHealth)
    @app.head("/health", include_in_schema=False)
    async def health() -> ServiceHealth:
        return ServiceHealth(service=service_name, status="ok", faults=faults.snapshot())

    @app.get("/metrics", response_model=MetricsSnapshot)
    async def get_metrics(request: Request) -> MetricsSnapshot:
        trace_id = getattr(request.state, "trace_id", None)
        return MetricsSnapshot(**metrics.snapshot(trace_id=trace_id))

    if service_name != "api-service":
        @app.get("/export/prometheus", response_class=PlainTextResponse)
        async def export_prometheus() -> PlainTextResponse:
            snapshot = metrics.snapshot()
            body = render_prometheus_metrics(
                service_name,
                [
                    ("service_up", "Service availability state", "gauge", 1),
                    ("service_requests_total", "Total HTTP requests handled", "counter", snapshot["request_count"]),
                    ("service_errors_total", "Total HTTP error responses handled", "counter", snapshot["error_count"]),
                    ("service_error_rate", "HTTP error rate", "gauge", snapshot["error_rate"]),
                    ("service_avg_latency_ms", "Average HTTP request latency in milliseconds", "gauge", snapshot["avg_latency_ms"]),
                    ("service_p95_latency_ms", "P95 HTTP request latency in milliseconds", "gauge", snapshot["p95_latency_ms"]),
                    ("service_uptime_seconds", "Service uptime in seconds", "gauge", snapshot["uptime_seconds"]),
                ],
            )
            return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")
 
    if service_name != "api-service":
        @app.get("/", include_in_schema=False)
        @app.head("/", include_in_schema=False)
        async def root_ping():
            return {"status": "ok", "service": service_name}

    @app.get("/faults")
    async def get_faults() -> dict[str, int | bool]:
        return faults.snapshot()

    @app.post("/faults/latency")
    async def set_latency(payload: FaultLatencyPayload) -> dict[str, int | bool]:
        state = faults.set_latency(payload.extra_latency_ms)
        log_event(
            logger,
            service=service_name,
            trace_id="system",
            event_type=EventType.RESPONSE_SENT,
            message="fault updated",
            extra={"fault": "latency", "state": state},
            level=30,
        )
        return state

    @app.post("/faults/memory-leak")
    async def set_memory_leak(payload: FaultMemoryLeakPayload) -> dict[str, int | bool]:
        state = faults.set_memory_leak(payload.enabled, payload.chunk_size_kb)
        log_event(
            logger,
            service=service_name,
            trace_id="system",
            event_type=EventType.RESPONSE_SENT,
            message="fault updated",
            extra={"fault": "memory_leak", "state": state},
            level=30,
        )
        return state

    @app.post("/faults/crash")
    async def crash() -> dict[str, str]:
        log_event(
            logger,
            service=service_name,
            trace_id="system",
            event_type=EventType.ERROR,
            message="fault triggered",
            error="crash",
            extra={"fault": "crash"},
            level=40,
        )
        faults.crash()
        return {"status": "crashing"}

    @app.post("/faults/reset")
    async def reset_faults() -> dict[str, int | bool]:
        state = faults.reset()
        log_event(
            logger,
            service=service_name,
            trace_id="system",
            event_type=EventType.RESPONSE_SENT,
            message="fault reset",
            extra={"state": state},
            level=30,
        )
        return state


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
