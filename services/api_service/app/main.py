import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any
import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse

from services.common.app.broker import get_stream_backend
from services.common.app.config import get_env
from services.common.app.contracts import DataResponse
from services.common.app.contracts import ExternalIngestPayload
from services.common.app.contracts import IngestResponse
from services.common.app.contracts import IngestTestResponse
from services.common.app.contracts import ValuePayload
from services.common.app.faults import FaultState
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import log_event
from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import TRACE_ID_HEADER
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan
from services.common.app.streaming import get_stream_publisher

SERVICE_NAME = get_env("SERVICE_NAME", "api-service")
AUTH_SERVICE_URL = get_env("AUTH_SERVICE_URL", "http://localhost:8007")
CACHE_SERVICE_URL = get_env("CACHE_SERVICE_URL", "http://localhost:8001")
DATABASE_SERVICE_URL = get_env("DATABASE_SERVICE_URL", "http://localhost:8002")
EVENT_PROCESSOR_SERVICE_URL = get_env("EVENT_PROCESSOR_SERVICE_URL", "http://localhost:8003")
RCA_SERVICE_URL = get_env("RCA_SERVICE_URL", "http://localhost:8005")
SYNTHETIC_GENERATOR_SERVICE_URL = get_env("SYNTHETIC_GENERATOR_SERVICE_URL", "http://localhost:8004")
PAYMENT_SERVICE_URL = get_env("PAYMENT_SERVICE_URL", "http://localhost:8008")
NOTIFICATION_SERVICE_URL = get_env("NOTIFICATION_SERVICE_URL", "http://localhost:8009")
LOGS_TOPIC = get_env("LOGS_TOPIC", "logs_topic")
METRICS_TOPIC = get_env("METRICS_TOPIC", "metrics_topic")
PIPELINE_LABEL = "logs_topic -> processor -> RCA"
PIPELINE_DISPLAY = "logs_topic -> processor -> RCA".replace("->", "\u2192")
EXPORT_SERVICE_URLS = {
    "api-service": get_env("API_SERVICE_URL", "http://localhost:8000"),
    "auth-service": AUTH_SERVICE_URL,
    "cache-service": CACHE_SERVICE_URL,
    "database-service": DATABASE_SERVICE_URL,
    "payment-service": PAYMENT_SERVICE_URL,
    "notification-service": NOTIFICATION_SERVICE_URL,
}

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
publisher = get_stream_publisher(logger)
stream_backend = get_stream_backend()
attach_common_routes(app, SERVICE_NAME, faults, metrics)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
INDEX_HTML_PATH = PROJECT_ROOT / "index.html"
SERVICE_URLS = {
    "auth-service": AUTH_SERVICE_URL,
    "cache-service": CACHE_SERVICE_URL,
    "database-service": DATABASE_SERVICE_URL,
    "payment-service": PAYMENT_SERVICE_URL,
    "notification-service": NOTIFICATION_SERVICE_URL,
}


async def _proxy_json(method: str, url: str, **kwargs) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.request(method, url, **kwargs)
    except httpx.RequestError as exc:
        # Diagnostic: Include the raw exception string to identify DNS/Port/Timeout issues
        raise HTTPException(status_code=502, detail=f"Upstream unavailable: {url} | Error: {str(exc)}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"Invalid upstream response: {url}") from exc

    return JSONResponse(status_code=response.status_code, content=payload)


def _ingest_payload(payload: ExternalIngestPayload, *, topic: str) -> dict:
    body = payload.model_dump(exclude_none=True)
    body["source"] = body.get("source", "external-ingest")
    if topic == LOGS_TOPIC:
        body["message"] = body.get("message", f"external {payload.event_type.lower()}")
    elif "metric_name" not in body and "metric_value" not in body:
        body["metric_name"] = "latency_ms"
        body["metric_value"] = payload.latency
    return body


async def _pipeline_trace_detected(trace_id: str) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            for _ in range(40):
                normalized_response = await client.get(f"{EVENT_PROCESSOR_SERVICE_URL}/normalized-events", params={"seconds": 10})
                if normalized_response.status_code == 200:
                    events = normalized_response.json().get("events", [])
                    if any(event.get("trace_id") == trace_id for event in events):
                        rca_response = await client.get(f"{RCA_SERVICE_URL}/rca/latest")
                        if rca_response.status_code == 200:
                            return True, rca_response.json().get("status", "UNKNOWN")
                        return True, "UNKNOWN"
                await asyncio.sleep(0.5)
    except httpx.RequestError:
        return False, "UNREACHABLE"
    return False, "TIMEOUT"


async def _metrics_snapshot_for(service: str, url: str) -> dict | None:
    endpoint = url.rstrip("/") + "/metrics"
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(endpoint, headers={TRACE_ID_HEADER: f"export-prometheus-{service}"})
            if response.status_code == 200:
                return response.json()
    except httpx.RequestError:
        return None
    return None


def _prometheus_text(snapshots: dict[str, dict]) -> str:
    lines = [
        "# HELP service_request_count Total requests observed by the service",
        "# TYPE service_request_count gauge",
    ]
    for service, snapshot in snapshots.items():
        lines.append(f'service_request_count{{service="{service}"}} {snapshot["request_count"]}')
    lines.extend(
        [
            "# HELP service_error_count Total errors observed by the service",
            "# TYPE service_error_count gauge",
        ]
    )
    for service, snapshot in snapshots.items():
        lines.append(f'service_error_count{{service="{service}"}} {snapshot["error_count"]}')
    lines.extend(
        [
            "# HELP service_error_rate Error rate observed by the service",
            "# TYPE service_error_rate gauge",
        ]
    )
    for service, snapshot in snapshots.items():
        lines.append(f'service_error_rate{{service="{service}"}} {snapshot["error_rate"]}')
    lines.extend(
        [
            "# HELP service_avg_latency_ms Average latency in milliseconds",
            "# TYPE service_avg_latency_ms gauge",
        ]
    )
    for service, snapshot in snapshots.items():
        lines.append(f'service_avg_latency_ms{{service="{service}"}} {snapshot["avg_latency_ms"]}')
    lines.extend(
        [
            "# HELP service_p95_latency_ms P95 latency in milliseconds",
            "# TYPE service_p95_latency_ms gauge",
        ]
    )
    for service, snapshot in snapshots.items():
        lines.append(f'service_p95_latency_ms{{service="{service}"}} {snapshot["p95_latency_ms"]}')
    lines.extend(
        [
            "# HELP service_uptime_seconds Service uptime in seconds",
            "# TYPE service_uptime_seconds gauge",
        ]
    )
    for service, snapshot in snapshots.items():
        lines.append(f'service_uptime_seconds{{service="{service}"}} {snapshot["uptime_seconds"]}')
    return "\n".join(lines) + "\n"


def _recent_log_events(limit: int) -> list[dict]:
    entries = stream_backend.recent_entries(LOGS_TOPIC, count=limit)
    events: list[dict] = []
    for stream_id, fields in entries:
        raw_payload = fields.get("payload")
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        events.append({"stream_id": stream_id, "payload": payload})
    return events


def _timestamp_to_ns(timestamp: str | None) -> str:
    if not timestamp:
        return "0"
    normalized = timestamp.replace("Z", "+00:00")
    try:
        return str(int(datetime.fromisoformat(normalized).timestamp() * 1_000_000_000))
    except ValueError:
        return "0"


async def _trace_events(trace_id: str, seconds: int) -> list[dict]:
    async with httpx.AsyncClient(timeout=2.0) as client:
        response = await client.get(
            f"{EVENT_PROCESSOR_SERVICE_URL}/normalized-events",
            params={"seconds": seconds},
            headers={TRACE_ID_HEADER: f"export-jaeger-{trace_id}"},
        )
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="event processor unavailable")
    events = response.json().get("events", [])
    return [event for event in events if event.get("trace_id") == trace_id]


def _trace_view_payload(trace_id: str, events: list[dict]) -> dict:
    ordered_events = sorted(events, key=lambda item: item.get("timestamp", ""))
    path: list[str] = []
    timestamps: list[str] = []
    seen_services: set[str] = set()
    for event in ordered_events:
        service = event.get("service")
        timestamp = event.get("timestamp")
        if not service or service in seen_services:
            continue
        seen_services.add(service)
        path.append(service)
        timestamps.append(timestamp)
    return {
        "trace_id": trace_id,
        "path": path,
        "timestamps": timestamps,
    }


async def _request_json(
    *,
    method: str,
    url: str,
    trace_id: str,
    json_payload: dict | None = None,
    timeout: float = 3.0,
) -> dict:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method,
                url,
                json=json_payload,
                headers={TRACE_ID_HEADER: trace_id},
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"upstream unavailable: {url}") from exc

    if response.status_code >= 500:
        raise HTTPException(status_code=502, detail=f"upstream failed: {url}")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text or f"request failed: {url}")

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"invalid upstream response: {url}") from exc


async def _set_service_fault(service: str, failure_type: str) -> dict[str, Any] | None:
    base_url = SERVICE_URLS.get(service)
    if not base_url:
        return None

    if failure_type == "latency":
        if service == "notification-service":
            endpoint = f"{base_url}/faults/notification-delay"
            payload = {"extra_latency_ms": 1500}
        else:
            endpoint = f"{base_url}/faults/latency"
            payload = {"extra_latency_ms": 1500}
    elif failure_type == "error":
        endpoint_map = {
            "auth-service": "/faults/auth-error",
            "cache-service": "/faults/cache-error",
            "database-service": "/faults/database-error",
            "payment-service": "/faults/payment-error",
            "notification-service": "/faults/notification-drop",
        }
        endpoint_suffix = endpoint_map.get(service)
        if not endpoint_suffix:
            return None
        endpoint = f"{base_url}{endpoint_suffix}"
        payload = {"enabled": True}
    else:
        return None

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.post(endpoint, json=payload)
    except httpx.RequestError:
        return {
            "status": "unavailable",
            "service": service,
            "failure_type": failure_type,
            "detail": "direct service fault endpoint unreachable; synthetic fallback still active",
        }
    if response.status_code >= 400:
        return {
            "status": "unavailable",
            "service": service,
            "failure_type": failure_type,
            "detail": "direct service fault endpoint rejected request; synthetic fallback still active",
        }
    return response.json()


async def _clear_service_fault(service: str, failure_type: str) -> None:
    base_url = SERVICE_URLS.get(service)
    if not base_url:
        return

    if failure_type == "latency":
        if service == "notification-service":
            endpoint = f"{base_url}/faults/notification-delay"
            payload = {"extra_latency_ms": 0}
        else:
            endpoint = f"{base_url}/faults/reset"
            payload = None
    elif failure_type == "error":
        endpoint_map = {
            "auth-service": ("/faults/auth-error", {"enabled": False}),
            "cache-service": ("/faults/cache-error", {"enabled": False}),
            "database-service": ("/faults/database-error", {"enabled": False}),
            "payment-service": ("/faults/payment-error", {"enabled": False}),
            "notification-service": ("/faults/notification-drop", {"enabled": False}),
        }
        endpoint_suffix, payload = endpoint_map.get(service, ("", None))
        if not endpoint_suffix:
            return
        endpoint = f"{base_url}{endpoint_suffix}"
    else:
        return

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            if payload is None:
                await client.post(endpoint)
            else:
                await client.post(endpoint, json=payload)
    except httpx.RequestError:
        return


async def _clear_fault_after_delay(service: str, failure_type: str, duration: int) -> None:
    await asyncio.sleep(duration)
    await _clear_service_fault(service, failure_type)


@app.get("/", response_class=FileResponse)
@app.head("/", include_in_schema=False)
async def root() -> FileResponse:
    return FileResponse(INDEX_HTML_PATH)


@app.get("/dashboard", response_class=FileResponse)
async def dashboard() -> FileResponse:
    return FileResponse(INDEX_HTML_PATH)


@app.get("/data", response_model=DataResponse)
async def get_data(
    request: Request,
    key: str = Query(default="alpha", min_length=1),
) -> DataResponse:
    trace_id = request.state.trace_id
    payload = await _request_json(
        method="GET",
        url=f"{CACHE_SERVICE_URL}/cache/{key}",
        trace_id=trace_id,
    )
    return DataResponse(
        key=payload["key"],
        value=payload["value"],
        source=payload["source"],
        served_by=SERVICE_NAME,
        cache_hit=payload.get("cache_hit"),
    )


@app.get("/login")
async def login(
    request: Request,
    user: str = Query(default="user:123", min_length=1),
) -> dict[str, Any]:
    trace_id = request.state.trace_id
    auth_payload = await _request_json(
        method="GET",
        url=f"{AUTH_SERVICE_URL}/auth/validate/{user}",
        trace_id=trace_id,
    )
    return {
        "service": SERVICE_NAME,
        "user": user,
        "authenticated": auth_payload.get("authorized", False),
        "auth_service": auth_payload.get("service"),
    }


@app.get("/purchase")
async def purchase(
    request: Request,
    order_id: str = Query(default="order-default", min_length=1),
) -> dict[str, Any]:
    trace_id = request.state.trace_id
    payload = await _request_json(
        method="GET",
        url=f"{PAYMENT_SERVICE_URL}/payments/purchase/{order_id}",
        trace_id=trace_id,
    )
    return {"service": SERVICE_NAME, **payload}


@app.get("/notify")
async def notify(
    request: Request,
    message_id: str = Query(default="notification-default", min_length=1),
) -> dict[str, Any]:
    trace_id = request.state.trace_id
    payload = await _request_json(
        method="POST",
        url=f"{NOTIFICATION_SERVICE_URL}/notifications/send/{message_id}",
        trace_id=trace_id,
    )
    return {"service": SERVICE_NAME, **payload}


@app.get("/items/{key}", response_model=DataResponse)
async def get_item(key: str, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"{AUTH_SERVICE_URL}/auth/items/{key}", headers={TRACE_ID_HEADER: trace_id})
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="auth request failed",
            path=f"/items/{key}",
            method="GET",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="auth service unavailable") from exc

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"{key} not found")
    if response.status_code >= 500:
        raise HTTPException(status_code=502, detail="auth service unavailable")

    payload = response.json()
    return DataResponse(
        key=payload["key"],
        value=payload["value"],
        source=payload["source"],
        served_by=SERVICE_NAME,
        cache_hit=payload.get("cache_hit"),
    )


@app.put("/seed/{key}", response_model=DataResponse)
async def seed_item(key: str, payload: ValuePayload, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            auth_response = await client.get(
                f"{AUTH_SERVICE_URL}/auth/validate/{key}",
                headers={TRACE_ID_HEADER: trace_id},
            )
            if auth_response.status_code >= 500:
                raise HTTPException(status_code=502, detail="auth service unavailable")
            db_response = await client.put(
                f"{DATABASE_SERVICE_URL}/db/{key}",
                json=payload.model_dump(),
                headers={TRACE_ID_HEADER: trace_id},
            )
            if db_response.status_code >= 500:
                raise HTTPException(status_code=502, detail="database service unavailable")

            cache_response = await client.put(
                f"{CACHE_SERVICE_URL}/cache/{key}",
                json={"value": payload.value, "source": "api"},
                headers={TRACE_ID_HEADER: trace_id},
            )
            if cache_response.status_code >= 500:
                raise HTTPException(status_code=502, detail="cache service unavailable")
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="downstream seed request failed",
            path=f"/seed/{key}",
            method="PUT",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="downstream service unavailable") from exc

    return DataResponse(key=key, value=payload.value, source="api", served_by=SERVICE_NAME, cache_hit=True)


@app.post("/ingest/log", response_model=IngestResponse, status_code=202)
async def ingest_log(payload: ExternalIngestPayload, request: Request) -> IngestResponse:
    trace_id = request.state.trace_id
    event = _ingest_payload(payload, topic=LOGS_TOPIC)
    publisher.publish_log(event)
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.RESPONSE_SENT,
        message="external log ingested",
        path="/ingest/log",
        method="POST",
        status_code=202,
        extra={"ingested_service": payload.service, "ingested_trace_id": payload.trace_id, "topic": LOGS_TOPIC},
    )
    return IngestResponse(
        status="accepted",
        topic=LOGS_TOPIC,
        service=payload.service,
        trace_id=payload.trace_id,
        pipeline=PIPELINE_DISPLAY,
    )


@app.post("/ingest/metric", response_model=IngestResponse, status_code=202)
async def ingest_metric(payload: ExternalIngestPayload, request: Request) -> IngestResponse:
    trace_id = request.state.trace_id
    event = _ingest_payload(payload, topic=METRICS_TOPIC)
    publisher.publish_metric(event)
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=trace_id,
        event_type=EventType.RESPONSE_SENT,
        message="external metric ingested",
        path="/ingest/metric",
        method="POST",
        status_code=202,
        extra={"ingested_service": payload.service, "ingested_trace_id": payload.trace_id, "topic": METRICS_TOPIC},
    )
    return IngestResponse(
        status="accepted",
        topic=METRICS_TOPIC,
        service=payload.service,
        trace_id=payload.trace_id,
        pipeline=PIPELINE_DISPLAY,
    )


@app.post("/debug/ingest-test", response_model=IngestTestResponse, status_code=202)
async def debug_ingest_test(request: Request) -> IngestTestResponse:
    trace_id = f"ingest-test-{request.state.trace_id}"
    payload = ExternalIngestPayload(
        service="api-service",
        event_type="ERROR",
        latency=1400,
        timestamp=utc_timestamp_ms(),
        trace_id=trace_id,
        error_rate=1.0,
        source="debug-ingest-test",
    )
    publisher.publish_log(_ingest_payload(payload, topic=LOGS_TOPIC))
    normalized_detected, rca_status = await _pipeline_trace_detected(trace_id)
    log_event(
        logger,
        service=SERVICE_NAME,
        trace_id=request.state.trace_id,
        event_type=EventType.RESPONSE_SENT,
        message="debug ingest test executed",
        path="/debug/ingest-test",
        method="POST",
        status_code=202,
        extra={"ingested_trace_id": trace_id, "normalized_detected": normalized_detected, "rca_status": rca_status},
    )
    return IngestTestResponse(
        status="accepted",
        pipeline=PIPELINE_DISPLAY,
        trace_id=trace_id,
        normalized_detected=normalized_detected,
        rca_status=rca_status,
    )


@app.get("/export/prometheus", response_class=PlainTextResponse)
async def export_prometheus() -> PlainTextResponse:
    snapshots_list = await asyncio.gather(
        *[_metrics_snapshot_for(service, url) for service, url in EXPORT_SERVICE_URLS.items()]
    )
    snapshots = {
        service: snapshot
        for service, snapshot in zip(EXPORT_SERVICE_URLS.keys(), snapshots_list, strict=False)
        if snapshot is not None
    }
    body = _prometheus_text(snapshots)
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/export/loki")
async def export_loki(limit: int = Query(default=100, ge=1, le=500)) -> JSONResponse:
    events = _recent_log_events(limit)
    streams: dict[str, list[list[str]]] = {}
    for event in events:
        payload = event["payload"]
        service = payload.get("service", "unknown")
        streams.setdefault(service, []).append(
            [
                _timestamp_to_ns(payload.get("timestamp")),
                json.dumps(payload, ensure_ascii=True),
            ]
        )
    return JSONResponse(
        {
            "status": "success",
            "pipeline": PIPELINE_DISPLAY,
            "note": "Compatibility export only. Redis logs_topic remains the primary pipeline.",
            "data": {
                "resultType": "streams",
                "result": [
                    {"stream": {"service": service, "source": "logs_topic"}, "values": values}
                    for service, values in streams.items()
                ],
            },
        }
    )


@app.get("/export/jaeger")
async def export_jaeger(
    trace_id: str = Query(..., min_length=1),
    seconds: int = Query(default=10, ge=1, le=10),
) -> JSONResponse:
    events = await _trace_events(trace_id, seconds)
    services_seen: list[str] = []
    spans: list[dict] = []
    for index, event in enumerate(sorted(events, key=lambda item: item.get("timestamp", ""))):
        service = event.get("service", "unknown")
        if service not in services_seen:
            services_seen.append(service)
        spans.append(
            {
                "spanID": f"{index + 1}",
                "operationName": event.get("event_type", "UNKNOWN"),
                "serviceName": service,
                "startTime": event.get("timestamp"),
                "tags": [
                    {"key": "status", "type": "string", "value": str(event.get("status"))},
                    {"key": "source_topic", "type": "string", "value": str(event.get("source_topic"))},
                    {"key": "latency", "type": "float64", "value": float(event.get("latency") or 0.0)},
                ],
            }
        )
    return JSONResponse(
        {
            "data": [
                {
                    "traceID": trace_id,
                    "spans": spans,
                    "processes": {
                        service: {"serviceName": service}
                        for service in services_seen
                    },
                    "flow": services_seen,
                    "pipeline": PIPELINE_DISPLAY,
                    "note": "Compatibility export only. Redis streams and existing trace propagation remain unchanged.",
                }
            ],
            "total": 1 if spans else 0,
        }
    )


@app.get("/trace/view")
async def trace_view(
    trace_id: str = Query(..., min_length=1),
    seconds: int = Query(default=10, ge=1, le=10),
) -> JSONResponse:
    events = await _trace_events(trace_id, seconds)
    payload = _trace_view_payload(trace_id, events)
    return JSONResponse(payload)


@app.get("/rca/latest")
async def proxy_latest_rca() -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/rca/latest")


@app.get("/rca/timeline")
async def proxy_rca_timeline() -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/rca/timeline")


@app.get("/normalized-events")
async def proxy_normalized_events(seconds: int = Query(default=5, ge=1, le=30)) -> JSONResponse:
    return await _proxy_json("GET", f"{EVENT_PROCESSOR_SERVICE_URL}/normalized-events", params={"seconds": seconds})


@app.get("/incidents")
async def proxy_incidents() -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/incidents")


@app.get("/incidents/{incident_id}/report")
async def proxy_incident_report(incident_id: str) -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/incidents/{incident_id}/report")


@app.get("/incidents/{incident_id}/download")
async def proxy_incident_download(incident_id: str):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{RCA_SERVICE_URL}/incidents/{incident_id}/download")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream unavailable: {RCA_SERVICE_URL}") from exc
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Incident not found")
    from fastapi.responses import Response
    return Response(
        content=response.content,
        media_type=response.headers.get("content-type", "application/pdf"),
        headers={
            "Content-Disposition": response.headers.get(
                "content-disposition",
                f'attachment; filename="incident_report_{incident_id}.pdf"',
            ),
        },
    )


@app.get("/incidents/{incident_id}")
async def proxy_incident_by_id(incident_id: str) -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/incidents/{incident_id}")


@app.get("/debug/latency")
async def proxy_debug_latency() -> JSONResponse:
    return await _proxy_json("GET", f"{RCA_SERVICE_URL}/debug/latency")


@app.post("/inject")
async def proxy_inject(request: Request) -> JSONResponse:
    payload = await request.json()
    service = payload.get("service")
    failure_type = payload.get("type")
    duration = int(payload.get("duration", 10))

    if service not in SERVICE_URLS:
        raise HTTPException(status_code=400, detail=f"unsupported service: {service}")
    if failure_type not in {"latency", "error", "crash"}:
        raise HTTPException(status_code=400, detail=f"unsupported failure type: {failure_type}")

    service_fault_response = None
    if failure_type in {"latency", "error"}:
        service_fault_response = await _set_service_fault(service, failure_type)
        asyncio.create_task(_clear_fault_after_delay(service, failure_type, duration))

    synthetic_payload = {
        **payload,
        "type": "crash" if failure_type == "crash" else failure_type,
        "duration": duration,
    }
    synthetic_response = await _proxy_json("POST", f"{SYNTHETIC_GENERATOR_SERVICE_URL}/inject", json=synthetic_payload)
    synthetic_body = json.loads(synthetic_response.body.decode())
    return JSONResponse(
        status_code=synthetic_response.status_code,
        content={
            **synthetic_body,
            "service_fault": service_fault_response,
        },
    )
