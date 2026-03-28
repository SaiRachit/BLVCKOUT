import json
import math
import queue
import threading
import time
from collections import defaultdict
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi import Query
from fastapi.responses import PlainTextResponse

from services.common.app.broker import get_stream_backend
from services.common.app.config import get_env
from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.metrics import render_prometheus_metrics
from services.common.app.plain_logging import get_plain_logger
from services.common.app.online_stats import OnlineStats, ExponentialMovingAverage

SERVICE_NAME = get_env("SERVICE_NAME", "event-processor-service")
LOGS_TOPIC = get_env("LOGS_TOPIC", "logs_topic")
METRICS_TOPIC = get_env("METRICS_TOPIC", "metrics_topic")
NORMALIZED_TOPIC = get_env("NORMALIZED_TOPIC", "normalized_events_topic")
HEALTH_TOPIC = get_env("HEALTH_TOPIC", "service_health_topic")
LATENCY_THRESHOLD_MS = float(get_env("LATENCY_THRESHOLD_MS", "500"))
ERROR_SPIKE_COUNT = int(get_env("ERROR_SPIKE_COUNT", "2"))
WINDOW_SECONDS = int(get_env("WINDOW_SECONDS", "3"))
ORDERING_BUFFER_SECONDS = float(get_env("ORDERING_BUFFER_SECONDS", "0.1"))
PROCESS_BATCH_SIZE = int(get_env("PROCESS_BATCH_SIZE", "16"))
QUEUE_MAXSIZE = int(get_env("QUEUE_MAXSIZE", "4096"))
LATENCY_TREND_THRESHOLD_MS = float(get_env("LATENCY_TREND_THRESHOLD_MS", "80"))

SERVICE_DEPENDENCY_GRAPH: dict[str, list[str]] = {
    "api-service": ["auth-service", "cache-service", "payment-service", "notification-service"],
    "auth-service": [],
    "cache-service": ["database-service"],
    "database-service": [],
    "payment-service": ["database-service"],
    "notification-service": [],
}


def parse_timestamp_to_epoch(timestamp: str | None) -> float:
    if not timestamp:
        return time.time()
    normalized = timestamp.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


@dataclass
class ServiceWindowState:
    recent_events: deque[dict[str, Any]]
    recent_errors: deque[float]
    recent_latencies: deque[tuple[float, float]]
    reorder_buffer: list[dict[str, Any]]
    latency_stats: OnlineStats
    error_interval_stats: OnlineStats
    ema_fast: ExponentialMovingAverage
    ema_slow: ExponentialMovingAverage
    last_error_time: float | None = None
    latest_status_update: str = "OK"


class EventProcessor:
    def __init__(self) -> None:
        self.logger = get_plain_logger(SERVICE_NAME)
        self.backend = get_stream_backend()
        self._events: deque[dict[str, Any]] = deque()
        self._service_states: dict[str, ServiceWindowState] = defaultdict(
            lambda: ServiceWindowState(
                recent_events=deque(),
                recent_errors=deque(),
                recent_latencies=deque(),
                reorder_buffer=[],
                latency_stats=OnlineStats(),
                error_interval_stats=OnlineStats(),
                ema_fast=ExponentialMovingAverage(alpha=0.3),
                ema_slow=ExponentialMovingAverage(alpha=0.1),
                latest_status_update="OK",
            )
        )
        self._lock = threading.RLock()
        initial_offset = "$" if self.backend.backend_name == "redis" else "0-0"
        self._stream_offsets = {LOGS_TOPIC: initial_offset, METRICS_TOPIC: initial_offset}
        self._ingest_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=QUEUE_MAXSIZE)
        self._rate_samples: deque[float] = deque()
        self._processing_latencies_ms: deque[float] = deque(maxlen=500)
        self._last_batch_size = 0
        self._dropped_events = 0
        self._last_event_received_at: str | None = None
        self._events_processed = 0
        self._consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._worker_thread = threading.Thread(target=self._process_loop, daemon=True)
        self._consumer_thread.start()
        self._worker_thread.start()

    def _consume_loop(self) -> None:
        while True:
            try:
                response = self.backend.read(self._stream_offsets, block_ms=250, count=PROCESS_BATCH_SIZE)
                for topic, entries in response:
                    for entry_id, fields in entries:
                        self._stream_offsets[topic] = entry_id
                        raw_payload = fields.get("payload")
                        if not raw_payload:
                            continue
                        envelope = {
                            "source_topic": topic,
                            "stream_id": entry_id,
                            "ingestion_timestamp": utc_timestamp_ms(),
                            "ingestion_epoch": time.time(),
                            "payload": json.loads(raw_payload),
                        }
                        self._last_event_received_at = envelope["ingestion_timestamp"]
                        self.logger.info(
                            json.dumps(
                                {
                                    "stage": "consumer_received",
                                    "stream": topic,
                                    "backend": self.backend.backend_name,
                                    "stream_id": entry_id,
                                    "raw": envelope["payload"],
                                },
                                ensure_ascii=True,
                            )
                        )
                        self._record_rate_sample(envelope["ingestion_epoch"])
                        try:
                            self._ingest_queue.put_nowait(envelope)
                        except queue.Full:
                            with self._lock:
                                self._dropped_events += 1
            except Exception:
                time.sleep(1)

    def _process_loop(self) -> None:
        while True:
            batch: list[dict[str, Any]] = []
            try:
                first = self._ingest_queue.get(timeout=0.2)
                batch.append(first)
            except queue.Empty:
                self._flush_ready_events(force=True)
                continue

            while len(batch) < PROCESS_BATCH_SIZE:
                try:
                    batch.append(self._ingest_queue.get_nowait())
                except queue.Empty:
                    break

            self._last_batch_size = len(batch)
            for envelope in batch:
                self._buffer_event(envelope)
                self._ingest_queue.task_done()
            self._flush_ready_events(force=False)

    def _buffer_event(self, envelope: dict[str, Any]) -> None:
        payload = envelope["payload"]
        service = payload["service"]
        event_timestamp_epoch = parse_timestamp_to_epoch(payload.get("timestamp"))
        buffered_event = {
            **envelope,
            "event_timestamp_epoch": event_timestamp_epoch,
        }
        with self._lock:
            state = self._service_states[service]
            state.reorder_buffer.append(buffered_event)
            state.reorder_buffer.sort(key=lambda item: item["event_timestamp_epoch"])

    def _flush_ready_events(self, *, force: bool) -> None:
        ready_events: list[dict[str, Any]] = []
        with self._lock:
            now = time.time()
            for state in self._service_states.values():
                retained: list[dict[str, Any]] = []
                for buffered in state.reorder_buffer:
                    delay = now - buffered["event_timestamp_epoch"]
                    if force or delay >= ORDERING_BUFFER_SECONDS:
                        ready_events.append(buffered)
                    else:
                        retained.append(buffered)
                state.reorder_buffer = retained

        ready_events.sort(key=lambda item: (item["payload"]["service"], item["event_timestamp_epoch"], item["stream_id"]))
        for envelope in ready_events:
            self._process_event(envelope)

    def _process_event(self, envelope: dict[str, Any]) -> None:
        payload = envelope["payload"]
        processing_epoch = time.time()
        processing_timestamp = utc_timestamp_ms()
        normalized = self._normalize(
            source_topic=envelope["source_topic"],
            event=payload,
            ingestion_timestamp=envelope["ingestion_timestamp"],
            ingestion_epoch=envelope["ingestion_epoch"],
            processing_timestamp=processing_timestamp,
            processing_epoch=processing_epoch,
        )
        self._store_event(normalized, processing_epoch)
        self._publish_normalized(normalized)
        self._record_processing_latency(normalized["processing_latency_ms"])
        self._events_processed += 1
        self.logger.info(
            json.dumps(
                {
                    "stage": "consumer_normalized",
                    "backend": self.backend.backend_name,
                    "normalized": normalized,
                },
                ensure_ascii=True,
            )
        )

    def _normalize(
        self,
        *,
        source_topic: str,
        event: dict[str, Any],
        ingestion_timestamp: str,
        ingestion_epoch: float,
        processing_timestamp: str,
        processing_epoch: float,
    ) -> dict[str, Any]:
        latency = event.get("latency")
        if latency is None and event.get("metric_name") == "latency_ms":
            latency = event.get("metric_value")
        latency_value = float(latency or 0.0)

        status, inference, ml_signal = self._infer_status(
            service=event["service"],
            source_topic=source_topic,
            event_type=event.get("event_type"),
            message=str(event.get("message", "")),
            latency=latency_value,
            error_rate=float(event.get("error_rate", 0.0)),
            event_timestamp_epoch=parse_timestamp_to_epoch(event.get("timestamp")),
        )

        return {
            "timestamp": event.get("timestamp", utc_timestamp_ms()),
            "service": event["service"],
            "trace_id": event.get("trace_id"),
            "event_type": event.get("event_type", "UNKNOWN"),
            "metric_name": event.get("metric_name"),
            "metric_value": event.get("metric_value"),
            "latency": latency_value,
            "status": status,
            "source_topic": source_topic,
            "ingestion_timestamp": ingestion_timestamp,
            "processing_timestamp": processing_timestamp,
            "processing_latency_ms": round((processing_epoch - ingestion_epoch) * 1000, 2),
            "inference": inference,
            "ml_signal": ml_signal,
        }

    def _infer_status(
        self,
        *,
        service: str,
        source_topic: str,
        event_type: str | None,
        message: str,
        latency: float,
        error_rate: float,
        event_timestamp_epoch: float,
    ) -> tuple[str, dict[str, Any], dict[str, float]]:
        with self._lock:
            state = self._service_states[service]
            self._trim_locked(time.time())

            state.recent_latencies.append((event_timestamp_epoch, latency))
            if latency > 0:
                state.latency_stats.push(latency)
                state.ema_fast.update(latency)
                state.ema_slow.update(latency)

            if event_type == "ERROR":
                state.recent_errors.append(event_timestamp_epoch)
                if state.last_error_time is not None:
                    state.error_interval_stats.push(event_timestamp_epoch - state.last_error_time)
                state.last_error_time = event_timestamp_epoch

            while state.recent_latencies and state.recent_latencies[0][0] < event_timestamp_epoch - WINDOW_SECONDS:
                state.recent_latencies.popleft()
            while state.recent_errors and state.recent_errors[0] < event_timestamp_epoch - WINDOW_SECONDS:
                state.recent_errors.popleft()

            recent_error_count = len(state.recent_errors)
            latency_trend = state.ema_fast.value - state.ema_slow.value
            high_latency = latency > LATENCY_THRESHOLD_MS
            error_spike = recent_error_count >= ERROR_SPIKE_COUNT or error_rate >= 0.5
            increasing_latency = latency_trend >= LATENCY_TREND_THRESHOLD_MS

            if error_spike and (high_latency or increasing_latency):
                status = "FAILED"
            elif error_spike:
                status = "FAILED"
            elif high_latency or increasing_latency:
                status = "DEGRADED"
            else:
                status = "OK"

            ml_signal = self._ml_signal(
                state=state,
                source_topic=source_topic,
                event_type=event_type,
                message=message,
                latency=latency,
                recent_error_count=recent_error_count,
            )
            state.latest_status_update = status
            health_payload = {
                "timestamp": utc_timestamp_ms(),
                "service": service,
                "status": status,
                "latency_threshold_ms": LATENCY_THRESHOLD_MS,
                "latency_trend_ms": round(latency_trend, 2),
                "error_spike_count": ERROR_SPIKE_COUNT,
                "recent_error_count": recent_error_count,
                "window_seconds": WINDOW_SECONDS,
            }
            try:
                self.backend.publish(HEALTH_TOPIC, health_payload)
            except Exception:
                pass

            return status, {
                "latency_trend_ms": round(latency_trend, 2),
                "recent_error_count": recent_error_count,
                "high_latency": high_latency,
                "increasing_latency": increasing_latency,
                "error_spike": error_spike,
            }, ml_signal

    def _ml_signal(
        self,
        *,
        state: ServiceWindowState,
        source_topic: str,
        event_type: str | None,
        message: str,
        latency: float,
        recent_error_count: int,
    ) -> dict[str, float]:
        log_score = 0.0
        metric_score = 0.0
        if source_topic == LOGS_TOPIC:
            signal_text = f"{event_type or ''} {message}".upper()
            keyword_hits = sum(1 for token in ("ERROR", "FAIL", "TIMEOUT", "RESET", "REFUSED") if token in signal_text)
            
            # Semantic weight (Simulated LogBERT pattern detection)
            semantic_patterns = [
                ("java.net.ConnectException", 0.95),
                ("Broken pipe", 0.85),
                ("Max retries exceeded", 0.8),
                ("503 Service Unavailable", 0.9),
                ("Upstream failure", 0.85),
            ]
            semantic_score = max([score for pattern, score in semantic_patterns if pattern.upper() in signal_text] or [0.0])
            keyword_score = min(1.0, 0.45 * keyword_hits + 0.3 * semantic_score)
            
            # Frequency spike (Log volume anomaly)
            frequency_score = min(1.0, recent_error_count / max(1, ERROR_SPIKE_COUNT))
            log_score = round(min(1.0, 0.6 * keyword_score + 0.4 * frequency_score), 3)
            
        if source_topic == METRICS_TOPIC:
            # Trend flags (Simulated TCN/LSTM predictive flag)
            ema_fast = state.ema_fast.value
            ema_slow = state.ema_slow.value
            
            # Sudden Trend Inversion
            trend_score = min(1.0, (ema_fast - ema_slow) / max(ema_slow, 50.0)) if ema_slow > 0 else 0.0
            z_dev = state.latency_stats.z_score(latency)
            threshold = self._get_dynamic_z_threshold()
            spike_score = min(1.0, z_dev / threshold) # Adaptive sigma threshold
            
            metric_score = round(min(1.0, 0.5 * trend_score + 0.5 * spike_score), 3)
            
        return {
            "distilbert_log_score": log_score,
            "lstm_metric_score": metric_score,
        }

    def _get_dynamic_z_threshold(self) -> float:
        """
        Calculates a dynamic Z-score threshold based on the number of active microservices.
        This follows the Bonferroni-style adjustment for multiple comparisons:
        Z = 2.0 + (0.65 * log10(N))
        """
        with self._lock:
            num_services = len(self._service_states)
        
        # Ensure we have at least one service to avoid log10(0)
        n = max(1, num_services)
        
        # Scaling constant 0.65 anchors the current 6-service system to Z=2.5
        return 2.0 + (0.65 * math.log10(n))

    def _latency_trend_ms(self, samples: deque[tuple[float, float]]) -> float:
        if len(samples) < 2:
            return 0.0
        midpoint = len(samples) // 2
        older = [latency for _, latency in list(samples)[:midpoint] if latency > 0]
        newer = [latency for _, latency in list(samples)[midpoint:] if latency > 0]
        if not older or not newer:
            return 0.0
        return (sum(newer) / len(newer)) - (sum(older) / len(older))

    def _store_event(self, event: dict[str, Any], now: float) -> None:
        with self._lock:
            stored = {**event, "_processed_at_epoch": now}
            self._events.append(stored)
            self._service_states[event["service"]].recent_events.append(stored)
            self._trim_locked(now)

    def _trim_locked(self, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        while self._events and self._events[0]["_processed_at_epoch"] < cutoff:
            self._events.popleft()
        while self._rate_samples and self._rate_samples[0] < cutoff:
            self._rate_samples.popleft()

        for state in self._service_states.values():
            while state.recent_events and state.recent_events[0]["_processed_at_epoch"] < cutoff:
                state.recent_events.popleft()
            while state.recent_errors and state.recent_errors[0] < cutoff:
                state.recent_errors.popleft()
            while state.recent_latencies and state.recent_latencies[0][0] < cutoff:
                state.recent_latencies.popleft()
            if not state.recent_events:
                state.latest_status_update = "OK"

    def _publish_normalized(self, normalized: dict[str, Any]) -> None:
        try:
            self.backend.publish(NORMALIZED_TOPIC, normalized)
        except Exception:
            pass

    def _record_rate_sample(self, timestamp_epoch: float) -> None:
        with self._lock:
            self._rate_samples.append(timestamp_epoch)
            self._trim_locked(timestamp_epoch)

    def _record_processing_latency(self, latency_ms: float) -> None:
        with self._lock:
            self._processing_latencies_ms.append(latency_ms)

    def events_in_window(self, seconds: int, service: str | None = None) -> list[dict[str, Any]]:
        now = time.time()
        cutoff = now - min(seconds, WINDOW_SECONDS)
        with self._lock:
            self._trim_locked(now)
            filtered = []
            for event in self._events:
                if event["_processed_at_epoch"] < cutoff:
                    continue
                if service and event["service"] != service:
                    continue
                filtered.append({k: v for k, v in event.items() if not k.startswith("_")})
            return filtered

    def health_states(self) -> list[dict[str, Any]]:
        with self._lock:
            self._trim_locked(time.time())
            return [
                {
                    "service": service,
                    "status": state.latest_status_update,
                    "recent_event_count": len(state.recent_events),
                    "recent_error_count": len(state.recent_errors),
                    "latency_trend_ms": round(self._latency_trend_ms(state.recent_latencies), 2),
                    "buffered_event_count": len(state.reorder_buffer),
                    "window_seconds": WINDOW_SECONDS,
                }
                for service, state in self._service_states.items()
            ]

    def debug_stream_rate(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            self._trim_locked(now)
            recent_count = len(self._rate_samples)
            active_span_seconds = max(1.0, now - self._rate_samples[0]) if self._rate_samples else 1.0
            rate_per_second = round(recent_count / active_span_seconds, 2)
            return {
                "window_seconds": WINDOW_SECONDS,
                "events_seen": recent_count,
                "events_per_second": rate_per_second,
                "active_span_seconds": round(active_span_seconds, 2),
                "queue_depth": self._ingest_queue.qsize(),
                "queue_capacity": QUEUE_MAXSIZE,
                "last_batch_size": self._last_batch_size,
                "dropped_events": self._dropped_events,
            }

    def debug_window_events(self, service: str | None = None, seconds: int = 10) -> dict[str, Any]:
        return {
            "window_seconds": min(seconds, WINDOW_SECONDS),
            "service": service,
            "events": self.events_in_window(seconds, service=service),
        }

    def debug_processing_latency(self) -> dict[str, Any]:
        with self._lock:
            samples = list(self._processing_latencies_ms)
        avg_latency = round(sum(samples) / len(samples), 2) if samples else 0.0
        p95_latency = 0.0
        over_sla_count = 0
        if samples:
            ordered = sorted(samples)
            index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
            p95_latency = round(ordered[index], 2)
            over_sla_count = len([value for value in ordered if value > 1000.0])
        return {
            "samples": len(samples),
            "avg_processing_latency_ms": avg_latency,
            "p95_processing_latency_ms": p95_latency,
            "max_processing_latency_ms": round(max(samples), 2) if samples else 0.0,
            "over_1s_count": over_sla_count,
            "within_target": over_sla_count == 0,
        }

    def pipeline_health(self) -> dict[str, Any]:
        producer_active = False
        if self._last_event_received_at:
            producer_active = (time.time() - parse_timestamp_to_epoch(self._last_event_received_at)) < 2.0
        return {
            "producer_active": producer_active,
            "consumer_active": self._consumer_thread.is_alive() and self._worker_thread.is_alive(),
            "last_event_received_at": self._last_event_received_at,
            "events_processed": self._events_processed,
            "backend": self.backend.backend_name,
        }


processor = EventProcessor()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "ok"}


@app.get("/normalized-events")
async def normalized_events(seconds: int = Query(default=10, ge=1, le=10)) -> dict[str, Any]:
    return {
        "window_seconds": seconds,
        "events": processor.events_in_window(seconds),
    }


@app.get("/service-health")
async def service_health() -> dict[str, Any]:
    return {
        "updated_at": utc_timestamp_ms(),
        "services": processor.health_states(),
        "dependency_graph": SERVICE_DEPENDENCY_GRAPH,
    }


@app.get("/debug/stream-rate")
async def debug_stream_rate() -> dict[str, Any]:
    return processor.debug_stream_rate()


@app.get("/debug/window-events")
async def debug_window_events(
    seconds: int = Query(default=10, ge=1, le=10),
    service: str | None = Query(default=None),
) -> dict[str, Any]:
    return processor.debug_window_events(service=service, seconds=seconds)


@app.get("/debug/processing-latency")
async def debug_processing_latency() -> dict[str, Any]:
    return processor.debug_processing_latency()


@app.get("/debug/pipeline-health")
async def debug_pipeline_health() -> dict[str, Any]:
    return processor.pipeline_health()


@app.get("/export/prometheus", response_class=PlainTextResponse)
async def export_prometheus() -> PlainTextResponse:
    latency = processor.debug_processing_latency()
    health = processor.pipeline_health()
    body = render_prometheus_metrics(
        SERVICE_NAME,
        [
            ("service_up", "Service availability state", "gauge", 1),
            (
                "event_processor_avg_processing_latency_ms",
                "Average event processing latency in milliseconds",
                "gauge",
                latency["avg_processing_latency_ms"],
            ),
            (
                "event_processor_p95_processing_latency_ms",
                "P95 event processing latency in milliseconds",
                "gauge",
                latency["p95_processing_latency_ms"],
            ),
            (
                "event_processor_max_processing_latency_ms",
                "Maximum event processing latency in milliseconds",
                "gauge",
                latency["max_processing_latency_ms"],
            ),
            (
                "event_processor_over_1s_total",
                "Events processed over one second latency",
                "counter",
                latency["over_1s_count"],
            ),
            (
                "event_processor_within_target",
                "Whether processing latency is currently within target",
                "gauge",
                latency["within_target"],
            ),
            (
                "event_processor_events_processed_total",
                "Total processed events",
                "counter",
                health["events_processed"],
            ),
            ("event_processor_producer_active", "Producer activity state", "gauge", health["producer_active"]),
            ("event_processor_consumer_active", "Consumer activity state", "gauge", health["consumer_active"]),
        ],
    )
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")
