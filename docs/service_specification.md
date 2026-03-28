# Simulated Distributed System Specification

This document defines the current REST contracts for the simulated distributed system and the planned streaming contracts for the later observability phase. Kafka is not implemented in this milestone.

Redis Streams are now used for the ingestion and normalization phase. Kafka remains the preferred production-scale option later, but the local implementation uses Redis stream topics for lower setup overhead.

## Service Overview

| Service | Purpose | Downstream Dependency | Port |
| --- | --- | --- | --- |
| API Service | Public entrypoint for clients; orchestrates reads through cache | Cache Service | 8000 |
| Cache Service | In-memory cache with DB fallback | Database Service | 8001 |
| Database Service | Simulated persistent store | None | 8002 |

## Current Runtime Flow

1. Client calls API Service.
2. API Service requests data from Cache Service.
3. Cache Service serves cached value or fetches from Database Service.
4. Each service emits standardized JSON logs and exposes local metrics at `GET /metrics`.

## Standard REST Endpoints

### Common Endpoints

| Endpoint | Method | Description | Response |
| --- | --- | --- | --- |
| `/health` | GET | Liveness plus fault state | `ServiceHealth` |
| `/metrics` | GET | Current request metrics snapshot | `MetricsSnapshot` |
| `/faults` | GET | Current fault injection settings | `FaultState` |
| `/faults/latency` | POST | Set artificial latency in milliseconds | `FaultState` |
| `/faults/memory-leak` | POST | Enable or disable simulated memory growth | `FaultState` |
| `/faults/crash` | POST | Crash the process after a short delay | `{"status":"crashing"}` |
| `/faults/reset` | POST | Reset latency and leak state | `FaultState` |

### API Service Endpoints

| Endpoint | Method | Description | Request | Response |
| --- | --- | --- | --- | --- |
| `/items/{key}` | GET | Fetch item through cache chain | Path param `key` | `ItemResponse` |
| `/seed/{key}` | PUT | Seed value into DB through orchestrated chain | Body `{"value": "..."}` | `SeedResponse` |

### Cache Service Endpoints

| Endpoint | Method | Description | Request | Response |
| --- | --- | --- | --- | --- |
| `/cache/{key}` | GET | Read from cache, fallback to DB | Path param `key` | `CacheResponse` |
| `/cache/{key}` | PUT | Update cache entry directly | Body `{"value": "...", "source": "api"}` | `CacheWriteResponse` |
| `/cache/clear` | POST | Clear all cache entries | None | `{"status":"cleared"}` |

### Database Service Endpoints

| Endpoint | Method | Description | Request | Response |
| --- | --- | --- | --- | --- |
| `/db/{key}` | GET | Read value from simulated DB | Path param `key` | `DatabaseResponse` |
| `/db/{key}` | PUT | Write value into simulated DB | Body `{"value":"..."}` | `DatabaseWriteResponse` |

## JSON Contracts

### `ServiceHealth`

```json
{
  "service": "api-service",
  "status": "ok",
  "faults": {
    "extra_latency_ms": 0,
    "memory_leak_enabled": false,
    "memory_leak_chunks": 0
  }
}
```

### `MetricsSnapshot`

```json
{
  "service": "cache-service",
  "request_count": 42,
  "error_count": 3,
  "error_rate": 0.0714,
  "avg_latency_ms": 12.3,
  "p95_latency_ms": 20.1,
  "uptime_seconds": 118
}
```

### `FaultState`

```json
{
  "extra_latency_ms": 250,
  "memory_leak_enabled": true,
  "memory_leak_chunks": 8
}
```

### `ItemResponse`

```json
{
  "key": "user:123",
  "value": "premium",
  "served_by": "api-service",
  "source": "cache"
}
```

### `CacheResponse`

```json
{
  "key": "user:123",
  "value": "premium",
  "source": "database",
  "cache_hit": false
}
```

### `DatabaseResponse`

```json
{
  "key": "user:123",
  "value": "premium",
  "source": "database"
}
```

## Logging Standard

Every service emits one JSON log line per request and for fault events with this envelope:

```json
{
  "timestamp": "2026-03-27T12:00:00.000Z",
  "service": "database-service",
  "level": "INFO",
  "message": "request completed",
  "request_id": "ef86e7b7e47a4d3cb4e1d651868b1a7a",
  "path": "/db/user:123",
  "method": "GET",
  "status_code": 200,
  "latency_ms": 4.8,
  "error": null
}
```

## Planned Streaming Contract For Later Observability Phase

These topics are now active for ingestion and normalization.

| Topic | Producers | Consumers | Partitions | Retention | Scaling Guidance |
| --- | --- | --- | --- | --- | --- |
| `sim.logs.json` | API, Cache, DB services | log normalizer, replay tools | 6 | 24 hours | Partition by `service` then `instance_id`; scale consumers horizontally by service groups |
| `sim.metrics.raw` | API, Cache, DB services | metrics normalizer, online feature builder | 6 | 24 hours | Partition by `service`; one consumer per downstream stage is enough for local dev |
| `sim.service.health` | API, Cache, DB services | dashboard, control plane | 3 | 12 hours | Low-volume topic; keep small partition count |
| `sim.fault.events` | API, Cache, DB services | dashboard, scenario runner | 3 | 7 days | Partition by `service`; preserve history for scenario playback |
| `sim.requests.trace` | API Service | future RCA pipeline | 6 | 24 hours | Partition by `request_id` to preserve request ordering across hops |

## Active Redis Stream Topics

| Stream | Producers | Consumer | Payload |
| --- | --- | --- | --- |
| `logs_topic` | API, Cache, DB services | Event Processor Service | structured log events |
| `metrics_topic` | API, Cache, DB services | Event Processor Service | metric snapshot and metric point events |
| `normalized_events_topic` | Event Processor Service | dashboard or future anomaly services | unified normalized events |
| `service_health_topic` | Event Processor Service | dashboard or future remediation services | inferred service health updates |
| synthetic generator output | Synthetic Generator Service | Event Processor Service | normal traffic, latency spikes, and error bursts |
| `normalized_events_topic` | Event Processor Service | RCA Service | normalized event stream for causal ordering |
| `service_health_topic` | Event Processor Service | RCA Service | sliding-window service health states |

## Unified Event Schema

```json
{
  "timestamp": "2026-03-27T12:00:00.123Z",
  "service": "cache-service",
  "trace_id": "trace-123",
  "event_type": "CACHE_HIT",
  "metric_name": "latency_ms",
  "metric_value": 18.2,
  "latency": 18.2,
  "status": "OK"
}
```

## State Inference Rules

1. `FAILED` if the service emits at least `3` error events inside the last `10` seconds.
2. `FAILED` if request volume is at least `3` in the current metric snapshot and `error_rate >= 0.5`.
3. `DEGRADED` if event latency exceeds `500 ms`.
4. Otherwise `OK`.

## Event Processor Reliability Rules

1. Events are buffered for about `0.35` seconds and ordered by event timestamp per service before processing.
2. The processor uses a true sliding window of the last `10` seconds and updates window state on every processed event.
3. Incoming stream records are decoupled from processing with an in-memory queue and small-batch draining.
4. Processing latency is tracked from ingestion to normalization and should remain under `1` second in normal load.

## Event Processor Debug Endpoints

| Endpoint | Purpose |
| --- | --- |
| `/debug/stream-rate` | shows current input rate, queue depth, last batch size, and dropped-event count |
| `/debug/window-events` | returns currently retained events from the active sliding window |
| `/debug/processing-latency` | reports average, p95, max, and SLA compliance for processing latency |
| `/debug/pipeline-health` | shows producer/consumer liveness, last event time, and processed count |

## RCA Rules

1. Dependency graph is fixed as `API -> Cache -> DB`.
2. RCA ranks the top hypotheses inside the active `10` second window instead of forcing a single explanation too early.
3. Confidence combines temporal consistency, dependency consistency, signal strength, repeated patterns, and noise/conflict penalties.
4. A propagation allowance of `2` seconds is used when correlating impacted dependent services.
5. When top candidates are close in score, RCA returns `UNCERTAIN` and includes alternative causes.

## RCA Endpoint

| Endpoint | Purpose |
| --- | --- |
| `/rca/latest` | returns the latest causal RCA result from the active sliding window |

### `/rca/latest` Response Highlights

`confidence_breakdown` and `confidence_explanation` are included to explain how RCA confidence was derived.

```json
{
  "incident_id": "database-service:2026-03-27T120000123Z:2026-03-27T120000140Z",
  "primary_root_cause": ["database-service"],
  "secondary_root_causes": [],
  "independent_failures": [],
  "confidence": 0.86,
  "confidence_breakdown": {
    "temporal_consistency": 1.0,
    "dependency_match": 1.0,
    "signal_strength": 0.87,
    "noise_penalty": 0.0,
    "impact_match": 1.0,
    "repeat_pattern_bonus": 0.25,
    "raw_score": 0.857,
    "final_confidence": 0.86
  },
  "confidence_explanation": "Confidence 0.86 for Database is based on strong temporal consistency, dependency graph alignment, high signal strength, low noise penalty.",
  "status": "CONFIDENT",
  "alternative_causes": [],
  "affected_services": ["cache-service", "api-service"],
  "missing_impact": [],
  "causal_chain": ["database-service -> cache-service -> api-service"],
  "reasoning": ["Database failed first", "Observed impact matches dependency graph"]
}
```

## Topic Payload Guidance

| Topic | Key | Value Shape |
| --- | --- | --- |
| `sim.logs.json` | `service` | standardized log envelope |
| `sim.metrics.raw` | `service` | metrics snapshot with timestamp |
| `sim.service.health` | `service` | `ServiceHealth` |
| `sim.fault.events` | `service` | fault change event with old/new state |
| `sim.requests.trace` | `request_id` | request lifecycle event for each hop |

## Why This Order

The right order for this milestone is:

1. Lock REST contracts and future event shapes.
2. Build independent services that already emit consistent logs and metrics.
3. Add observability transport later without rewriting service behavior.

That keeps this milestone focused on the simulator while avoiding contract drift before Kafka is introduced.
