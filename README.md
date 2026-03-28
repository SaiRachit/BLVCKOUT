# Simulated Distributed System

Six application services and three observability/support services simulate a distributed call chain:

- API Service
- Auth Service
- Cache Service
- Database Service
- Payment Service
- Notification Service
- Event Processor Service
- Synthetic Generator Service
- RCA Service

Current scope:

- REST-only service simulation
- end-to-end dependency chain: `api-service -> auth-service -> cache-service -> database-service -> payment-service -> notification-service`
- standardized JSON logging
- local per-service metrics endpoints
- Redis Streams ingestion on `logs_topic` and `metrics_topic`
- external ingestion APIs on the API service for direct log and metric publishing
- centralized normalization and service state inference
- optional synthetic high-rate traffic generation for demo fallback
- real-time causal RCA over the active 10-second window
- lightweight ML-style anomaly scoring in normalization and RCA outputs
- fault injection for latency spikes, payment failures, notification delays, process crash, and memory leak simulation

Redis Streams are used as the streaming backbone for this milestone. The planned topic contracts are documented in [docs/service_specification.md](/c:/Users/lenovo/OneDrive/문서/VS%20code/hac/docs/service_specification.md).

## Run With Docker

```bash
docker compose up --build
```

## Deploy on Railway

This app is **multiple containers** (plus Redis). Use **`RAILWAY.md`**, **`railway.toml`**, and **`env.railway.example`** to configure each Railway service and its variables.

Disable the synthetic generator if you want to drive the pipeline only with external data:

```bash
SYNTHETIC_GENERATOR_ENABLED=false docker compose up --build
```

Services:

- API: `http://localhost:8000`
- Cache: `http://localhost:8001`
- Database: `http://localhost:8002`
- Event Processor: `http://localhost:8003`
- Synthetic Generator: `http://localhost:8004`
- RCA: `http://localhost:8005`
- Remediation: `http://localhost:8006`
- Auth: `http://localhost:8007`
- Payment: `http://localhost:8008`
- Notification: `http://localhost:8009`

## Run As Separate Processes

Install dependencies:

```bash
pip install -r requirements.txt
```

Locust is included in `requirements.txt`, so the same install also enables load generation with:

```bash
locust -f locustfile.py
```

Start each service in a separate terminal:

```bash
uvicorn services.notification_service.app.main:app --host 0.0.0.0 --port 8009
uvicorn services.payment_service.app.main:app --host 0.0.0.0 --port 8008
uvicorn services.database_service.app.main:app --host 0.0.0.0 --port 8002
uvicorn services.cache_service.app.main:app --host 0.0.0.0 --port 8001
uvicorn services.auth_service.app.main:app --host 0.0.0.0 --port 8007
uvicorn services.api_service.app.main:app --host 0.0.0.0 --port 8000
uvicorn services.event_processor_service.app.main:app --host 0.0.0.0 --port 8003
uvicorn services.synthetic_generator_service.app.main:app --host 0.0.0.0 --port 8004
uvicorn services.rca_service.app.main:app --host 0.0.0.0 --port 8005
uvicorn services.remediation_service.app.main:app --host 0.0.0.0 --port 8006
```

## Example Requests

```bash
curl http://localhost:8000/items/user:123
curl -X PUT http://localhost:8000/seed/user:123 -H "Content-Type: application/json" -d "{\"value\":\"premium\"}"
curl http://localhost:8007/auth/validate/user:123
curl http://localhost:8008/payments/process/user:123
curl -X POST http://localhost:8009/notifications/send/user:123
curl -X POST http://localhost:8001/faults/latency -H "Content-Type: application/json" -d "{\"extra_latency_ms\":500}"
curl -X POST http://localhost:8007/faults/latency -H "Content-Type: application/json" -d "{\"extra_latency_ms\":700}"
curl -X POST http://localhost:8008/faults/payment-error -H "Content-Type: application/json" -d "{\"enabled\":true}"
curl -X POST http://localhost:8009/faults/notification-delay -H "Content-Type: application/json" -d "{\"extra_latency_ms\":400}"
curl -X POST http://localhost:8000/ingest/log -H "Content-Type: application/json" -d "{\"service\":\"payment-service\",\"event_type\":\"ERROR\",\"latency\":120,\"timestamp\":\"2026-03-27T14:00:00Z\",\"trace_id\":\"trace-1\"}"
curl -X POST http://localhost:8000/ingest/metric -H "Content-Type: application/json" -d "{\"service\":\"payment-service\",\"event_type\":\"ERROR\",\"latency\":120,\"timestamp\":\"2026-03-27T14:00:00Z\",\"trace_id\":\"trace-1\"}"
curl -X POST http://localhost:8000/debug/ingest-test
curl http://localhost:8002/metrics
curl http://localhost:8003/normalized-events?seconds=10
curl http://localhost:8003/service-health
curl http://localhost:8003/debug/pipeline-health
curl http://localhost:8005/rca/latest
python scripts/verify_pipeline.py
```

## Service Graph

Primary request path:

`api-service -> auth-service -> cache-service -> database-service -> payment-service -> notification-service`

Observability path:

`logs -> logs_topic -> event-processor-service -> rca-service`

`metrics -> metrics_topic -> event-processor-service -> rca-service`

Each runtime service:

- emits structured logs and metrics automatically
- propagates `trace_id` on downstream calls
- exposes `/health` and `/metrics`

## Observability Compatibility

The system can integrate with Prometheus, Loki, and Jaeger via these endpoints on the API service:

- `/export/prometheus`
- `/export/loki`
- `/export/jaeger?trace_id=<trace-id>`

These endpoints are compatibility exports layered on top of the current platform. They do not replace the existing Redis Streams pipeline.

Prometheus compatibility:

- `GET /export/prometheus`
- returns metrics in Prometheus text exposition format
- aggregates live service metrics from the running service chain

Loki compatibility:

- `GET /export/loki`
- returns recent structured logs grouped by service
- sourced from `logs_topic` so the existing log flow remains unchanged

Jaeger compatibility:

- `GET /export/jaeger?trace_id=<trace-id>`
- returns the observed trace flow across services for the provided `trace_id`
- derived from normalized pipeline events and propagated request trace IDs

Example commands:

```bash
curl http://localhost:8000/export/prometheus
curl http://localhost:8000/export/loki
curl "http://localhost:8000/export/jaeger?trace_id=locust-fetch-42-1234"
curl "http://localhost:8000/trace/view?trace_id=locust-fetch-42-1234"
```

Trace view compatibility:

- `GET /trace/view?trace_id=<trace-id>`
- returns the ordered service path and timestamps for the observed request journey
- uses the existing propagated `trace_id` values already flowing through the system

## Load Testing With Locust

The repo includes [locustfile.py](/c:/Users/lenovo/OneDrive/문서/VS%20code/hac/locustfile.py) for realistic traffic generation across the distributed chain.

Traffic mix:

- login flow: `auth-service /auth/validate/{key}`
- fetch flow: `api-service /items/{key}`
- payment flow: `payment-service /payments/process/{key}`

What this gives you:

- natural request-driven logs and metrics from live service traffic
- repeated `trace_id` propagation across downstream calls
- continuous events into `logs_topic` and `metrics_topic` through the existing runtime instrumentation

Default target scale:

- users: `75`
- supported range: `50` to `100`
- default spawn rate: `10`

Run with the web UI:

```bash
locust -f locustfile.py
```

Then open `http://localhost:8089` and start a test with:

- users: `50` to `100`
- spawn rate: any rate you want, for example `10`

Run headless:

```bash
locust -f locustfile.py --headless -u 75 -r 10 -t 5m
```

Configurable endpoints:

- `LOCUST_API_HOST` default: `http://localhost:8000`
- `LOCUST_AUTH_HOST` default: `http://localhost:8007`
- `LOCUST_PAYMENT_HOST` default: `http://localhost:8008`
- `LOCUST_USER_KEYSPACE` default: `250`

Example with explicit hosts and higher concurrency:

```bash
LOCUST_API_HOST=http://localhost:8000 \
LOCUST_AUTH_HOST=http://localhost:8007 \
LOCUST_PAYMENT_HOST=http://localhost:8008 \
locust -f locustfile.py --headless -u 100 -r 20 -t 5m
```

## External Integration

External systems can publish operational signals by calling the API service and sending the same event shape your observability agents or app hooks already produce. The API service validates the payload, writes it into `logs_topic` or `metrics_topic`, and the existing event processor and RCA services continue from there unchanged.

Minimal required fields:

- `service`
- `event_type`
- `latency`
- `timestamp`
- `trace_id`

Timestamp requirements:

- Must be ISO 8601
- Example: `2026-03-27T14:00:00Z`

Example log ingestion:

```bash
curl -X POST http://localhost:8000/ingest/log \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment-service",
    "event_type": "ERROR",
    "latency": 120,
    "timestamp": "2026-03-27T14:00:00Z",
    "trace_id": "payment-trace-001"
  }'
```

Example metric ingestion:

```bash
curl -X POST http://localhost:8000/ingest/metric \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment-service",
    "event_type": "ERROR",
    "latency": 120,
    "timestamp": "2026-03-27T14:00:00Z",
    "trace_id": "payment-trace-001"
  }'
```

Expected acceptance response:

```json
{
  "status": "accepted",
  "pipeline": "logs_topic -> processor -> RCA"
}
```

Debug pipeline test:

```bash
curl -X POST http://localhost:8000/debug/ingest-test
```

This endpoint publishes a sample external error event, waits briefly for normalization, and confirms the pipeline path using the same ingestion surface external integrations use.

## Failure Simulation

The expanded dependency chain includes lightweight fault hooks for RCA and demo scenarios:

- Auth latency spike: `POST /faults/latency` on `auth-service`
- Payment error: `POST /faults/payment-error` on `payment-service`
- Notification delay: `POST /faults/notification-delay` on `notification-service`

Example auth latency spike:

```bash
curl -X POST http://localhost:8007/faults/latency \
  -H "Content-Type: application/json" \
  -d "{\"extra_latency_ms\":700}"
```

Example payment failure:

```bash
curl -X POST http://localhost:8008/faults/payment-error \
  -H "Content-Type: application/json" \
  -d "{\"enabled\":true}"
```

Example notification delay:

```bash
curl -X POST http://localhost:8009/faults/notification-delay \
  -H "Content-Type: application/json" \
  -d "{\"extra_latency_ms\":400}"
```
