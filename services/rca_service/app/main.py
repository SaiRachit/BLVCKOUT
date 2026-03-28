import json
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

from services.common.app.broker import get_stream_backend
from services.common.app.config import get_env
from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.metrics import render_prometheus_metrics
from services.common.app.plain_logging import get_plain_logger

SERVICE_NAME = get_env("SERVICE_NAME", "rca-service")
NORMALIZED_TOPIC = get_env("NORMALIZED_TOPIC", "normalized_events_topic")
HEALTH_TOPIC = get_env("HEALTH_TOPIC", "service_health_topic")
REMEDIATION_TOPIC = get_env("REMEDIATION_TOPIC", "remediation_triggers_topic")
REMEDIATION_RESULTS_TOPIC = get_env("REMEDIATION_RESULTS_TOPIC", "remediation_results_topic")
WINDOW_SECONDS = int(get_env("WINDOW_SECONDS", "4"))
PROPAGATION_DELAY_SECONDS = float(get_env("PROPAGATION_DELAY_SECONDS", "1.2"))
RCA_HISTORY_SIZE = int(get_env("RCA_HISTORY_SIZE", "20"))
INCIDENT_HISTORY_SIZE = int(get_env("INCIDENT_HISTORY_SIZE", "10"))
UNCERTAIN_MARGIN = float(get_env("RCA_UNCERTAIN_MARGIN", "0.12"))
LATENCY_HISTORY_SIZE = int(get_env("LATENCY_HISTORY_SIZE", "200"))
PIPELINE_SLA_MS = float(get_env("PIPELINE_SLA_MS", "15000"))
INCIDENT_HISTORY_LIMIT = int(get_env("INCIDENT_HISTORY_LIMIT", "100"))
INCIDENT_INDEX_KEY = "incidents:list"
CONFIDENCE_THRESHOLD = float(get_env("CONFIDENCE_THRESHOLD", "0.6"))

DEPENDENCY_GRAPH: dict[str, list[str]] = {
    "api-service": ["auth-service", "cache-service", "payment-service", "notification-service"],
    "auth-service": [],
    "cache-service": ["database-service"],
    "database-service": [],
    "payment-service": ["database-service"],
    "notification-service": [],
}

DEPENDENTS_GRAPH: dict[str, list[str]] = {
    "notification-service": ["api-service"],
    "payment-service": ["api-service"],
    "database-service": ["cache-service", "payment-service"],
    "cache-service": ["api-service"],
    "auth-service": ["api-service"],
    "api-service": [],
}

SEVERITY_RANK = {
    "FAILED": 2,
    "DEGRADED": 1,
    "OK": 0,
}

PREVENTION_RULES: dict[str, list[str]] = {
    "database-service": [
        "Scale database horizontally with read replicas",
        "Optimize slow queries and add query execution plans",
        "Add connection pooling (e.g. PgBouncer)",
        "Introduce a caching layer for frequent reads",
        "Set up automated failover with health checks",
    ],
    "cache-service": [
        "Increase cache TTL for stable entries",
        "Add fallback to database on cache miss",
        "Use distributed cache (e.g. Redis Cluster)",
        "Monitor cache eviction rate and memory usage",
        "Implement circuit breaker for cache failures",
    ],
    "api-service": [
        "Add rate limiting per client/IP",
        "Enable horizontal autoscaling based on CPU/latency",
        "Optimize request handling with async processing",
        "Introduce circuit breaker for downstream calls",
        "Add request timeout and retry budgets",
    ],
    "auth-service": [
        "Add token caching to reduce auth lookup latency",
        "Implement graceful degradation for auth failures",
        "Set up redundant auth service instances",
        "Add health-check probes with automatic restarts",
    ],
    "payment-service": [
        "Implement idempotent payment processing",
        "Add payment retry queue with exponential backoff",
        "Set up transaction monitoring and alerting",
        "Add circuit breaker for payment gateway calls",
    ],
    "notification-service": [
        "Add notification retry queue with dead-letter fallback",
        "Implement async notification delivery",
        "Set up delivery rate limiting to prevent floods",
        "Add fallback notification channels",
    ],
}

DEFAULT_PREVENTION_SUGGESTIONS: list[str] = [
    "Add health-check probes with automatic restarts",
    "Implement circuit breaker pattern for dependencies",
    "Set up horizontal autoscaling based on load metrics",
    "Add structured alerting for anomaly detection",
]


def generate_prevention_suggestions(
    root_cause: str | None,
    affected_services: list[str] | None = None,
) -> list[str]:
    """Generate 3-5 rule-based prevention suggestions."""
    if not root_cause:
        return DEFAULT_PREVENTION_SUGGESTIONS[:3]
    base = PREVENTION_RULES.get(root_cause, DEFAULT_PREVENTION_SUGGESTIONS)
    suggestions = list(base[:4])
    if affected_services:
        for svc in affected_services:
            extras = PREVENTION_RULES.get(svc, [])
            for extra in extras:
                if extra not in suggestions and len(suggestions) < 5:
                    suggestions.append(extra)
    return suggestions[:5]


def parse_timestamp_to_epoch(timestamp: str | None) -> float:
    if not timestamp:
        return time.time()
    normalized = timestamp.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


@dataclass
class Hypothesis:
    service: str
    score: float
    confidence: float
    temporal_consistency: float
    dependency_match: float
    signal_strength: float
    noise_penalty: float
    impact_match: float
    repeated_pattern_bonus: float
    affected_services: list[str]
    missing_impact: list[str]
    independent_failures: list[str]
    causal_chain: list[str]
    reasoning: list[str]
    first_timestamp: str
    first_ingestion_timestamp: str | None
    anomaly_detection_timestamp: str | None
    log_signal: float
    metric_signal: float
    trace_signal: float
    status: str
    valid: bool


class RcaEngine:
    def __init__(self) -> None:
        self.logger = get_plain_logger(SERVICE_NAME)
        self.backend = get_stream_backend()
        self._events: deque[dict[str, Any]] = deque()
        self._health: dict[str, dict[str, Any]] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=RCA_HISTORY_SIZE)
        self._latency_history: deque[dict[str, Any]] = deque(maxlen=LATENCY_HISTORY_SIZE)
        self._last_triggered_incident_id: str | None = None
        self._triggered_incidents: dict[str, str] = {}
        self._incident_timings: dict[str, dict[str, Any]] = {}
        self._active_incident_ids: set[str] = set()
        self._incident_records: dict[str, dict[str, Any]] = {}
        self._incident_order: deque[str] = deque()
        self._incident_history: deque[dict[str, Any]] = deque(maxlen=INCIDENT_HISTORY_SIZE)
        self._incident_remediation: dict[str, dict[str, Any]] = {}
        self._redis_client = getattr(self.backend, "_client", None)
        self._latest: dict[str, Any] = {
            "primary_root_cause": [],
            "secondary_root_causes": [],
            "independent_failures": [],
            "ml_summary": {"log_signal": 0.0, "metric_signal": 0.0, "trace_signal": 0.0},
            "confidence": 0.0,
            "confidence_breakdown": {},
            "confidence_explanation": "No RCA candidate yet",
            "status": "UNCERTAIN",
            "reason": None,
            "alternative_causes": [],
            "affected_services": [],
            "missing_impact": [],
            "causal_chain": [],
            "reasoning": ["No RCA candidate yet"],
            "incident_id": None,
            "timings": {},
            "evaluated_at": utc_timestamp_ms(),
            "window_seconds": WINDOW_SECONDS,
            "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
        }
        self._lock = threading.Lock()
        initial_offset = "$" if self.backend.backend_name == "redis" else "0-0"
        self._offsets = {
            NORMALIZED_TOPIC: initial_offset,
            HEALTH_TOPIC: initial_offset,
            REMEDIATION_RESULTS_TOPIC: initial_offset,
        }
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self) -> None:
        while True:
            try:
                response = self.backend.read(self._offsets, block_ms=250, count=128)
                for topic, entries in response:
                    for entry_id, fields in entries:
                        self._offsets[topic] = entry_id
                        raw_payload = fields.get("payload")
                        if not raw_payload:
                            continue
                        payload = json.loads(raw_payload)
                        if topic == NORMALIZED_TOPIC:
                            self._record_event(payload)
                        elif topic == HEALTH_TOPIC:
                            self._record_health(payload)
                        elif topic == REMEDIATION_RESULTS_TOPIC:
                            self._record_remediation_result(payload)
                self._recompute_latest()
            except Exception:
                time.sleep(0.5)

    def _record_event(self, payload: dict[str, Any]) -> None:
        event = {
            **payload,
            "_event_epoch": parse_timestamp_to_epoch(payload.get("timestamp")),
        }
        with self._lock:
            self._events.append(event)
            self._trim_locked(time.time())

    def _record_health(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._health[payload["service"]] = payload

    def _record_remediation_result(self, payload: dict[str, Any]) -> None:
        incident_id = payload.get("incident_id")
        if not incident_id:
            return
        with self._lock:
            self._incident_remediation[incident_id] = dict(payload)
            existing_record = self._incident_records.get(incident_id)
        if not existing_record:
            return
        self._persist_incident(
            incident_id=incident_id,
            rca_payload=existing_record.get("rca"),
            timeline_payload=existing_record.get("timeline"),
            remediation_payload=dict(payload),
            created_at=existing_record.get("created_at"),
        )

    def _trim_locked(self, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        while self._events and self._events[0]["_event_epoch"] < cutoff:
            self._events.popleft()
        while self._history and parse_timestamp_to_epoch(self._history[0]["evaluated_at"]) < cutoff:
            self._history.popleft()
        stale_incidents = [
            incident_id
            for incident_id, timings in self._incident_timings.items()
            if parse_timestamp_to_epoch(timings.get("event_ingestion_timestamp")) < cutoff
            and incident_id not in self._active_incident_ids
        ]
        for incident_id in stale_incidents:
            self._incident_timings.pop(incident_id, None)
            self._triggered_incidents.pop(incident_id, None)

    def _recompute_latest(self) -> None:
        with self._lock:
            now = time.time()
            self._trim_locked(now)
            events = list(self._events)
            health = dict(self._health)

        degraded_or_failed = {
            service: info
            for service, info in health.items()
            if info.get("status") in {"DEGRADED", "FAILED"}
        }
        if not degraded_or_failed:
            result = {
                "primary_root_cause": [],
                "secondary_root_causes": [],
                "independent_failures": [],
                "ml_summary": {"log_signal": 0.0, "metric_signal": 0.0, "trace_signal": 0.0},
                "confidence": 0.0,
                "confidence_breakdown": {},
                "confidence_explanation": "All services are healthy in the active window",
                "status": "UNCERTAIN",
                "reason": None,
                "alternative_causes": [],
                "affected_services": [],
                "missing_impact": [],
                "causal_chain": [],
                "reasoning": ["All services are healthy in the active window"],
                "incident_id": None,
                "timings": {},
                "evaluated_at": utc_timestamp_ms(),
                "window_seconds": WINDOW_SECONDS,
                "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
            }
            with self._lock:
                self._latest = result
            return

        hypotheses = self._build_hypotheses(events, health, degraded_or_failed)
        if not hypotheses:
            with self._lock:
                self._active_incident_ids = set()
            result = {
                "primary_root_cause": [],
                "secondary_root_causes": [],
                "independent_failures": [],
                "ml_summary": {"log_signal": 0.0, "metric_signal": 0.0, "trace_signal": 0.0},
                "confidence": 0.12,
                "confidence_breakdown": {},
                "confidence_explanation": "No candidate could explain downstream impact in the active sliding window",
                "status": "UNCERTAIN",
                "reason": None,
                "alternative_causes": [],
                "affected_services": list(degraded_or_failed.keys()),
                "missing_impact": [],
                "causal_chain": [],
                "reasoning": ["No candidate could explain downstream impact in the active sliding window"],
                "incident_id": None,
                "timings": {},
                "evaluated_at": utc_timestamp_ms(),
                "window_seconds": WINDOW_SECONDS,
                "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
            }
            with self._lock:
                self._latest = result
                self._history.append(result)
            return

        ordered = sorted(hypotheses, key=lambda item: item.score, reverse=True)
        evaluated_at = utc_timestamp_ms()
        self._sync_incident_timings(hypotheses, evaluated_at)
        primary = ordered[0]
        independent_failures = primary.independent_failures
        secondary_root_causes = self._secondary_roots(primary, ordered[1:])
        alternatives = [
            candidate for candidate in ordered[1:3]
            if candidate.service not in secondary_root_causes and candidate.service not in independent_failures
        ]
        uncertain = (
            (not primary.valid)
            or bool(primary.missing_impact)
            or bool(alternatives and primary.score - alternatives[0].score <= UNCERTAIN_MARGIN)
        )
        if primary.missing_impact:
            status = "PARTIAL_PROPAGATION"
        elif secondary_root_causes or independent_failures:
            status = "MULTI_CAUSE"
        elif uncertain:
            status = "UNCERTAIN"
        else:
            status = "CONFIDENT"
        reasoning = list(primary.reasoning)
        if primary.missing_impact:
            reasoning.append(
                f"Missing downstream impact: {', '.join(primary.missing_impact)}"
            )
        if secondary_root_causes:
            reasoning.append(
                f"Secondary root causes detected: {', '.join(secondary_root_causes)}"
            )
        if independent_failures:
            reasoning.append(
                f"Independent failures detected: {', '.join(independent_failures)}"
            )
        if alternatives:
            reasoning.append(
                f"Alternative hypothesis: {alternatives[0].service} scored {alternatives[0].confidence}"
            )
        reason = None
        if primary.confidence < CONFIDENCE_THRESHOLD:
            status = "SUPPRESSED"
            reason = "Low confidence"
            reasoning.append(
                f"Suppressed because confidence {primary.confidence:.2f} is below threshold {CONFIDENCE_THRESHOLD:.2f}"
            )
        incident_id = self._incident_id(primary)
        timings = self._build_timing_snapshot(incident_id, primary, evaluated_at)

        result = {
            "incident_id": incident_id,
            "primary_root_cause": [primary.service],
            "secondary_root_causes": secondary_root_causes,
            "independent_failures": independent_failures,
            "ml_summary": {
                "log_signal": primary.log_signal,
                "metric_signal": primary.metric_signal,
                "trace_signal": primary.trace_signal,
            },
            "confidence": primary.confidence,
            "confidence_breakdown": {
                "temporal_consistency": primary.temporal_consistency,
                "dependency_match": primary.dependency_match,
                "signal_strength": primary.signal_strength,
                "noise_penalty": primary.noise_penalty,
                "impact_match": primary.impact_match,
                "trace_signal": primary.trace_signal,
                "repeat_pattern_bonus": primary.repeated_pattern_bonus,
                "raw_score": round(
                    0.2 * primary.temporal_consistency
                    + 0.2 * primary.dependency_match
                    + 0.6 * primary.signal_strength
                    + primary.noise_penalty,
                    3,
                ),
                "final_confidence": primary.confidence,
            },
            "confidence_explanation": self._confidence_explanation(
                primary,
                alternatives[0] if alternatives else None,
            ),
            "status": status,
            "reason": reason,
            "alternative_causes": [
                {"service": candidate.service, "confidence": candidate.confidence}
                for candidate in alternatives
            ],
            "affected_services": primary.affected_services,
            "missing_impact": primary.missing_impact,
            "causal_chain": primary.causal_chain,
            "reasoning": reasoning,
            "timings": timings,
            "evaluated_at": evaluated_at,
            "window_seconds": WINDOW_SECONDS,
            "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
        }
        remediation_triggered = self._trigger_remediation_if_needed(result)
        result["timings"]["remediation_triggered_at"] = remediation_triggered
        result["timings"]["total_pipeline_time_ms"] = self._duration_ms(
            result["timings"].get("event_ingestion_timestamp"),
            remediation_triggered,
        )
        result["timings"]["detection_to_rca_time_ms"] = self._duration_ms(
            result["timings"].get("anomaly_detection_timestamp"),
            result["timings"].get("rca_computation_timestamp"),
        )
        result["timings"]["status"] = "suppressed" if result.get("status") == "SUPPRESSED" else ("completed" if remediation_triggered else "active")
        self._incident_timings[incident_id] = dict(result["timings"])
        self._record_latency_sample(result)
        if result.get("status") == "SUPPRESSED":
            self._log_suppressed_event(result)
        timeline_payload = self._timeline_for(result, events).get("timeline", [])
        with self._lock:
            remediation_payload = dict(self._incident_remediation.get(incident_id, {}))
        self._persist_incident(
            incident_id=incident_id,
            rca_payload=result,
            timeline_payload=timeline_payload,
            remediation_payload=remediation_payload,
            created_at=result.get("evaluated_at"),
        )
        with self._lock:
            self._latest = result
            self._history.append(result)

    def _build_hypotheses(
        self,
        events: list[dict[str, Any]],
        health: dict[str, dict[str, Any]],
        degraded_or_failed: dict[str, dict[str, Any]],
    ) -> list[Hypothesis]:
        hypotheses: list[Hypothesis] = []
        trace_signals = self._trace_summary(events)
        for service in degraded_or_failed:
            service_events = [
                event for event in events if event["service"] == service and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not service_events:
                continue
            first_event = min(service_events, key=lambda item: item["_event_epoch"])
            affected_services, missing_impact_candidates, causal_chain, impact_match_score, valid = self._propagation_assessment(service, events, first_event, trace_signals)
            temporal_consistency = self._temporal_consistency(service, first_event, events)
            dependency_match = self._dependency_consistency(service, first_event, events)
            signal_strength = self._signal_strength(service, first_event, events, health.get(service, {}), trace_signals.get(service, 0.0))
            log_signal, metric_signal = self._ml_summary(service, events)
            trace_signal = trace_signals.get(service, 0.0)
            repeated_patterns = self._repeated_pattern_bonus(service)
            missing_impact, independent_failures = self._classify_missing_or_independent(
                events,
                missing_impact_candidates,
            )
            noise_penalty = self._noise_penalty(
                service=service,
                first_event=first_event,
                events=events,
                missing_impact=missing_impact,
                valid=valid,
            )
            direct_failure_bonus = self._direct_failure_bonus(
                service,
                first_event,
                affected_services,
            )

            raw_score = (
                0.2 * temporal_consistency
                + 0.2 * dependency_match
                + 0.6 * signal_strength
                + noise_penalty
                + direct_failure_bonus
            )
            confidence = round(max(0.0, min(1.0, raw_score)), 2)
            reasoning = self._reasoning(
                service=service,
                first_event=first_event,
                affected_services=affected_services,
                missing_impact=missing_impact,
                causal_chain=causal_chain,
                valid=valid,
                impact_match_score=impact_match_score,
                independent_failures=independent_failures,
                events=events,
            )
            hypotheses.append(
                Hypothesis(
                    service=service,
                    score=confidence,
                    confidence=confidence,
                    temporal_consistency=round(temporal_consistency, 2),
                    dependency_match=round(dependency_match, 2),
                    signal_strength=round(signal_strength, 2),
                    noise_penalty=round(noise_penalty, 2),
                    impact_match=round(impact_match_score, 2),
                    repeated_pattern_bonus=round(repeated_patterns + direct_failure_bonus, 2),
                    affected_services=affected_services,
                    missing_impact=missing_impact,
                    independent_failures=independent_failures,
                    causal_chain=causal_chain,
                    reasoning=reasoning,
                    first_timestamp=first_event["timestamp"],
                    first_ingestion_timestamp=first_event.get("ingestion_timestamp"),
                    anomaly_detection_timestamp=first_event.get("processing_timestamp"),
                    log_signal=log_signal,
                    metric_signal=metric_signal,
                    trace_signal=trace_signal,
                    status=first_event.get("status", "DEGRADED"),
                    valid=valid,
                )
            )
        return [hypothesis for hypothesis in hypotheses if hypothesis.confidence > 0.0]

    def _propagation_assessment(
        self,
        service: str,
        events: list[dict[str, Any]],
        first_event: dict[str, Any],
        trace_signals: dict[str, float],
    ) -> tuple[list[str], list[str], list[str], float, bool]:
        root_epoch = first_event["_event_epoch"]
        expected = self._all_downstream(service)
        observed: list[str] = []
        missing: list[str] = []
        
        # Build enriched chain with metrics (currently only showing names as per user request)
        chain_nodes = [service]
        
        for depth, downstream in enumerate(expected, start=1):
            downstream_events = [
                event for event in events if event["service"] == downstream and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not downstream_events:
                missing.append(downstream)
                continue
            first_downstream = min(downstream_events, key=lambda item: item["_event_epoch"])
            if root_epoch <= first_downstream["_event_epoch"] <= root_epoch + PROPAGATION_DELAY_SECONDS * depth:
                observed.append(downstream)
                chain_nodes.append(downstream)
            else:
                missing.append(downstream)

        if not expected:
            return observed, missing, [" -> ".join(chain_nodes)], 0.8, True

        match_score = len(observed) / len(expected)
        valid = len(observed) > 0
        return observed, missing, [" -> ".join(chain_nodes)], round(match_score, 2), valid

    def _all_downstream(self, service: str) -> list[str]:
        order: list[str] = []
        queue: deque[str] = deque(DEPENDENTS_GRAPH.get(service, []))
        while queue:
            current = queue.popleft()
            order.append(current)
            for child in DEPENDENTS_GRAPH.get(current, []):
                queue.append(child)
        return order

    def _temporal_consistency(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        upstream = [
            event
            for event in events
            if event["service"] in DEPENDENCY_GRAPH.get(service, [])
            and event.get("status") in {"FAILED", "DEGRADED"}
        ]
        if not upstream:
            return 1.0
        conflicts = len(
            [
                event
                for event in upstream
                if event["_event_epoch"] <= first_event["_event_epoch"] + PROPAGATION_DELAY_SECONDS
            ]
        )
        return max(0.0, 1.0 - 0.5 * conflicts)

    def _dependency_consistency(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        if service == "api-service" and (
            first_event.get("status") == "FAILED" or first_event.get("event_type") == "ERROR"
        ):
            return 1.0
        upstream = [
            event
            for event in events
            if event["service"] in DEPENDENCY_GRAPH.get(service, [])
            and event.get("status") in {"FAILED", "DEGRADED"}
            and event["_event_epoch"] < first_event["_event_epoch"]
        ]
        return 1.0 if not upstream else 0.35

    def _direct_failure_bonus(
        self,
        service: str,
        first_event: dict[str, Any],
        affected_services: list[str],
    ) -> float:
        if service != "api-service":
            return 0.0
        if affected_services:
            return 0.0
        if first_event.get("status") == "FAILED" or first_event.get("event_type") == "ERROR":
            return 0.16
        if first_event.get("status") == "DEGRADED":
            return 0.08
        return 0.0

    def _signal_strength(
        self,
        service: str,
        event: dict[str, Any],
        events: list[dict[str, Any]],
        health: dict[str, Any],
        trace_signal: float,
    ) -> float:
        service_events = [item for item in events if item.get("service") == service]
        anomaly_events = [
            item for item in service_events
            if item.get("status") in {"FAILED", "DEGRADED"} or item.get("event_type") == "ERROR"
        ]
        error_events = [item for item in anomaly_events if item.get("event_type") == "ERROR"]
        failed_events = [item for item in anomaly_events if item.get("status") == "FAILED"]
        latency_spikes = [item for item in anomaly_events if float(item.get("latency") or 0.0) > 500.0]
        log_signal, metric_signal = self._ml_summary(service, events)

        error_score = min(1.0, len(error_events) / 5.0)
        failure_score = min(1.0, len(failed_events) / 3.0)
        latency_score = min(1.0, len(latency_spikes) / 4.0)
        health_error_score = min(1.0, float(health.get("recent_error_count", 0)) / 10.0)
        first_event_bonus = 0.1 if event.get("status") == "FAILED" else 0.05 if event.get("status") == "DEGRADED" else 0.0

        combined = (
            0.28 * error_score
            + 0.27 * failure_score
            + 0.15 * latency_score
            + 0.1 * health_error_score
            + 0.15 * log_signal
            + 0.15 * metric_signal
            + 0.3 * trace_signal
            + first_event_bonus
        )
        return round(max(0.0, min(1.0, combined)), 2)

    def _ml_summary(self, service: str, events: list[dict[str, Any]]) -> tuple[float, float]:
        service_events = [item for item in events if item.get("service") == service]
        log_signal = 0.0
        metric_signal = 0.0
        for event in service_events:
            ml_signal = event.get("ml_signal", {}) or {}
            log_signal = max(log_signal, float(ml_signal.get("distilbert_log_score", 0.0) or 0.0))
            metric_signal = max(metric_signal, float(ml_signal.get("lstm_metric_score", 0.0) or 0.0))
        return round(log_signal, 3), round(metric_signal, 3)

    def _trace_summary(self, events: list[dict[str, Any]]) -> dict[str, float]:
        traces: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            tid = event.get("trace_id")
            if not tid:
                continue
            if tid not in traces:
                traces[tid] = []
            traces[tid].append(event)

        trace_root_counts: dict[str, int] = {}
        trace_participation_counts: dict[str, int] = {}
        total_error_traces = 0

        for tid, trace_events in traces.items():
            failing = [
                e for e in trace_events 
                if e.get("status") in {"FAILED", "DEGRADED"} or e.get("event_type") == "ERROR"
            ]
            if not failing:
                continue
                
            total_error_traces += 1
            # Patient Zero for this trace
            root_event = min(failing, key=lambda item: item.get("_event_epoch", 0.0))
            root_service = root_event.get("service", "unknown")
            trace_root_counts[root_service] = trace_root_counts.get(root_service, 0) + 1
            
            # Record participation
            participating_services = {e.get("service", "unknown") for e in trace_events}
            for svc in participating_services:
                trace_participation_counts[svc] = trace_participation_counts.get(svc, 0) + 1

        signals: dict[str, float] = {}
        for svc, count in trace_root_counts.items():
            # Signal score is root frequency relative to participation frequency
            participation = trace_participation_counts.get(svc, 1)
            signals[svc] = round(count / participation, 3)
            
        return signals

    def _repeated_pattern_bonus(self, service: str) -> float:
        if not self._history:
            return 0.0
        matches = 0
        for item in self._history:
            roots = item.get("primary_root_cause", [])
            if isinstance(roots, list) and service in roots:
                matches += 1
        return min(1.0, matches / 4.0)

    def _noise_factor(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        competing = [
            event
            for event in events
            if event["service"] != service
            and event.get("status") in {"FAILED", "DEGRADED"}
            and abs(event["_event_epoch"] - first_event["_event_epoch"]) <= PROPAGATION_DELAY_SECONDS
            and event["service"] not in DEPENDENCY_GRAPH.get(service, [])
            and event["service"] not in DEPENDENTS_GRAPH.get(service, [])
        ]
        return min(1.0, len(competing) / 3.0)

    def _noise_penalty(
        self,
        *,
        service: str,
        first_event: dict[str, Any],
        events: list[dict[str, Any]],
        missing_impact: list[str],
        valid: bool,
    ) -> float:
        conflicting_signals = self._noise_factor(service, first_event, events)
        missing_signal_penalty = min(0.2, 0.08 * len(missing_impact))
        validity_penalty = 0.08 if not valid else 0.0
        competing_root_penalty = min(0.12, 0.04 * len({
            event.get("service")
            for event in events
            if event.get("service") != service
            and event.get("status") in {"FAILED", "DEGRADED"}
            and abs(event.get("_event_epoch", 0.0) - first_event.get("_event_epoch", 0.0)) <= PROPAGATION_DELAY_SECONDS
        }))
        penalty = min(0.45, 0.15 * conflicting_signals + missing_signal_penalty + validity_penalty + competing_root_penalty)
        return round(-penalty, 2)

    def _classify_missing_or_independent(
        self,
        events: list[dict[str, Any]],
        missing_impact_candidates: list[str],
    ) -> tuple[list[str], list[str]]:
        missing_signals: list[str] = []
        independent: list[str] = []
        for candidate in missing_impact_candidates:
            candidate_events = [
                event
                for event in events
                if event["service"] == candidate and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not candidate_events:
                missing_signals.append(candidate)
                continue
            candidate_first = min(candidate_events, key=lambda item: item["_event_epoch"])
            upstream_failures = [
                event
                for event in events
                if event["service"] in DEPENDENCY_GRAPH.get(candidate, [])
                and event.get("status") in {"FAILED", "DEGRADED"}
                and event["_event_epoch"] < candidate_first["_event_epoch"]
            ]
            if not upstream_failures:
                independent.append(candidate)
            else:
                missing_signals.append(candidate)
        return missing_signals, independent

    def _secondary_roots(self, primary: Hypothesis, candidates: list[Hypothesis]) -> list[str]:
        secondaries: list[str] = []
        for candidate in candidates:
            if candidate.service in DEPENDENCY_GRAPH.get(primary.service, []) or candidate.service in DEPENDENTS_GRAPH.get(primary.service, []):
                continue
            if candidate.score >= 0.45:
                secondaries.append(candidate.service)
        return secondaries[:2]

    def _confidence_explanation(self, primary: Hypothesis, alternative: Hypothesis | None) -> str:
        chain = primary.causal_chain[0] if primary.causal_chain else self._display_name(primary.service)
        propagation_text = "clear propagation to dependents" if primary.affected_services else "limited downstream propagation"
        signal_text = (
            "strong error and failure signals" if primary.signal_strength >= 0.7
            else "moderate anomaly signals" if primary.signal_strength >= 0.4
            else "weak anomaly signals"
        )
        noise_text = (
            "low noise" if primary.noise_penalty >= -0.08
            else "noticeable competing noise" if primary.noise_penalty >= -0.2
            else "high noise from conflicting or missing signals"
        )
        explanation = (
            f"Confidence {primary.confidence:.2f}: {self._display_name(primary.service)} appears to be the root cause "
            f"with {propagation_text} along {chain}, {signal_text}, and {noise_text}."
        )
        if alternative:
            margin = round(primary.confidence - alternative.confidence, 3)
            explanation += (
                f" Next-best alternative is {self._display_name(alternative.service)} "
                f"(confidence {alternative.confidence:.2f}, confidence gap {margin:.3f})."
            )
        return explanation

    def _reasoning(
        self,
        *,
        service: str,
        first_event: dict[str, Any],
        affected_services: list[str],
        missing_impact: list[str],
        causal_chain: list[str],
        valid: bool,
        impact_match_score: float,
        independent_failures: list[str],
        events: list[dict[str, Any]],
    ) -> list[str]:
        reasoning = [f"{self._label(service)} failed first"]
        if affected_services:
            for affected in affected_services:
                affected_events = [
                    event
                    for event in events
                    if event["service"] == affected and event.get("status") in {"FAILED", "DEGRADED"}
                ]
                if affected_events:
                    reasoning.append(
                        f"{self._label(affected)} degraded after {self._label(service)} failure"
                    )
            reasoning.append("Observed impact matches dependency graph")
        else:
            reasoning.append("No downstream impact was observed inside the propagation window")
        if missing_impact:
            for missing in missing_impact:
                if missing in independent_failures:
                    reasoning.append(f"{self._label(missing)} has its own anomaly signal and is treated as an independent failure")
                else:
                    reasoning.append(f"{self._label(missing)} has no downstream anomaly signal and is treated as missing_signal")
        if causal_chain:
            reasoning.append(f"Dependency chain: {causal_chain[0]}")
        if not valid:
            reasoning.append("Candidate remains uncertain because it does not explain downstream impact")
        elif impact_match_score < 1.0:
            reasoning.append("Some expected downstream impact is missing, reducing confidence")
        return reasoning

    def _label(self, service: str) -> str:
        return service.replace("-service", "")

    def _incident_id(self, hypothesis: Hypothesis) -> str:
        base_timestamp = hypothesis.first_ingestion_timestamp or hypothesis.first_timestamp or utc_timestamp_ms()
        timestamp_ms = int(parse_timestamp_to_epoch(base_timestamp) * 1000.0)
        return f"inc_{timestamp_ms}_{hypothesis.service}"

    def _incident_key(self, incident_id: str) -> str:
        return f"incident:{incident_id}"

    def _persist_incident(
        self,
        *,
        incident_id: str,
        rca_payload: dict[str, Any] | None,
        timeline_payload: list[dict[str, Any]] | None,
        remediation_payload: dict[str, Any] | None,
        created_at: str | None,
    ) -> None:
        with self._lock:
            existing = self._incident_records.get(incident_id, {})
            incident_record = {
                "incident_id": incident_id,
                "rca": rca_payload if rca_payload is not None else dict(existing.get("rca", {})),
                "timeline": timeline_payload if timeline_payload is not None else list(existing.get("timeline", [])),
                "remediation": remediation_payload if remediation_payload is not None else dict(existing.get("remediation", {})),
                "created_at": created_at or existing.get("created_at") or utc_timestamp_ms(),
            }
            self._incident_records[incident_id] = incident_record
            if incident_id in self._incident_order:
                self._incident_order.remove(incident_id)
            self._incident_order.appendleft(incident_id)
            snapshot = self._incident_snapshot(incident_record)
            self._replace_incident_history_snapshot(snapshot)
            while len(self._incident_order) > INCIDENT_HISTORY_LIMIT:
                dropped = self._incident_order.pop()
                self._incident_records.pop(dropped, None)

        client = self._redis_client
        if client is None:
            return
        try:
            client.set(self._incident_key(incident_id), json.dumps(incident_record, ensure_ascii=True))
            client.lrem(INCIDENT_INDEX_KEY, 0, incident_id)
            client.lpush(INCIDENT_INDEX_KEY, incident_id)
            overflow = client.lrange(INCIDENT_INDEX_KEY, INCIDENT_HISTORY_LIMIT, -1)
            if overflow:
                for overflow_incident_id in overflow:
                    client.delete(self._incident_key(overflow_incident_id))
            client.ltrim(INCIDENT_INDEX_KEY, 0, INCIDENT_HISTORY_LIMIT - 1)
        except Exception:
            return

    def list_incidents(self) -> list[str]:
        client = self._redis_client
        if client is not None:
            try:
                return [str(item) for item in client.lrange(INCIDENT_INDEX_KEY, 0, INCIDENT_HISTORY_LIMIT - 1)]
            except Exception:
                pass
        with self._lock:
            return list(self._incident_order)

    def incident_history(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._incident_history]

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        client = self._redis_client
        if client is not None:
            try:
                payload = client.get(self._incident_key(incident_id))
                if payload:
                    return json.loads(payload)
            except Exception:
                pass
        with self._lock:
            record = self._incident_records.get(incident_id)
            if record is None:
                return None
            return dict(record)

    def get_incident_report(self, incident_id: str) -> dict[str, Any] | None:
        """Build a full incident report with prevention suggestions."""
        incident = self.get_incident(incident_id)
        if incident is None:
            return None
        rca = incident.get("rca", {}) or {}
        remediation = incident.get("remediation", {}) or {}
        root_causes = rca.get("primary_root_cause", []) or []
        root_cause = root_causes[0] if root_causes else None
        affected = list(rca.get("affected_services", []))
        timings = rca.get("timings", {})
        ml_summary = rca.get("ml_summary", {})
        remediation_status = remediation.get("status")
        remediation_action = remediation.get("action")
        outcome_text = remediation.get("details", {}).get("error")
        if not outcome_text:
            if remediation_status == "success":
                outcome_text = remediation.get("expected_recovery") or "Remediation executed successfully"
            elif remediation_status == "cooldown_skipped":
                outcome_text = remediation.get("reason") or "Remediation skipped due to cooldown"
            elif remediation_status == "failed":
                outcome_text = remediation.get("reason") or "Remediation failed"
            elif rca.get("status") == "SUPPRESSED":
                outcome_text = rca.get("reason") or "Incident suppressed due to low confidence"
            else:
                outcome_text = "Awaiting remediation outcome"
        suggestions = generate_prevention_suggestions(root_cause, affected)
        return {
            "incident_id": incident_id,
            "timestamp": rca.get("evaluated_at") or incident.get("created_at"),
            "root_cause": root_cause,
            "confidence": rca.get("confidence", 0.0),
            "status": rca.get("status", "UNCERTAIN"),
            "causal_chain": list(rca.get("causal_chain", [])),
            "reasoning": list(rca.get("reasoning", [])),
            "confidence_breakdown": dict(rca.get("confidence_breakdown", {})),
            "confidence_explanation": rca.get("confidence_explanation"),
            "ml_summary": dict(ml_summary) if ml_summary else {"log_signal": 0.0, "metric_signal": 0.0, "trace_signal": 0.0},
            "affected_services": affected,
            "action_taken": (
                f"{remediation_action} ({remediation_status or 'pending'})"
                if remediation_action
                else "No remediation action recorded"
            ),
            "outcome": outcome_text,
            "latency_change": {
                "before": timings.get("event_ingestion_timestamp"),
                "after": timings.get("remediation_triggered_at"),
            },
            "prevention_suggestions": suggestions,
        }

    def _replace_incident_history_snapshot(self, snapshot: dict[str, Any]) -> None:
        filtered = [item for item in self._incident_history if item.get("incident_id") != snapshot.get("incident_id")]
        filtered.insert(0, snapshot)
        self._incident_history = deque(filtered[:INCIDENT_HISTORY_SIZE], maxlen=INCIDENT_HISTORY_SIZE)

    def _incident_snapshot(self, incident_record: dict[str, Any]) -> dict[str, Any]:
        rca = incident_record.get("rca", {}) or {}
        remediation = incident_record.get("remediation", {}) or {}
        root_causes = rca.get("primary_root_cause", []) or []
        remediation_status = remediation.get("status")
        remediation_action = remediation.get("action")
        outcome = remediation.get("details", {}).get("error")
        if not outcome:
            if remediation_status == "success":
                outcome = remediation.get("expected_recovery") or "Remediation executed successfully"
            elif remediation_status == "cooldown_skipped":
                outcome = remediation.get("reason") or "Remediation skipped due to cooldown"
            elif remediation_status == "failed":
                outcome = remediation.get("reason") or "Remediation failed"
            elif rca.get("status") == "SUPPRESSED":
                outcome = rca.get("reason") or "Incident suppressed due to low confidence"
            else:
                outcome = "Awaiting remediation outcome"

        ml_summary = rca.get("ml_summary", {})
        timings = rca.get("timings", {})
        root_cause_service = root_causes[0] if root_causes else None
        affected = list(rca.get("affected_services", []))

        return {
            "incident_id": incident_record.get("incident_id"),
            "timestamp": rca.get("evaluated_at") or incident_record.get("created_at"),
            "root_cause": root_cause_service,
            "confidence": rca.get("confidence", 0.0),
            "status": rca.get("status", "UNCERTAIN"),
            "affected_services": affected,
            "causal_chain": list(rca.get("causal_chain", [])),
            "reasoning": list(rca.get("reasoning", [])),
            "confidence_breakdown": dict(rca.get("confidence_breakdown", {})),
            "confidence_reasoning": rca.get("confidence_explanation"),
            "ml_summary": dict(ml_summary) if ml_summary else {"log_signal": 0.0, "metric_signal": 0.0, "trace_signal": 0.0},
            "remediation_action": remediation_action,
            "remediation_status": remediation_status or ("suppressed" if rca.get("status") == "SUPPRESSED" else "pending"),
            "outcome": outcome,
            "latency_before": timings.get("event_ingestion_timestamp"),
            "latency_after": timings.get("remediation_triggered_at"),
        }

    def _build_timing_snapshot(self, incident_id: str, hypothesis: Hypothesis, evaluated_at: str) -> dict[str, Any]:
        existing = self._incident_timings.get(incident_id, {})
        rca_timestamp = existing.get("rca_computation_timestamp", evaluated_at)
        return {
            "event_ingestion_timestamp": existing.get("event_ingestion_timestamp", hypothesis.first_ingestion_timestamp),
            "anomaly_detection_timestamp": existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
            "rca_computation_timestamp": rca_timestamp,
            "detection_to_rca_time_ms": self._duration_ms(
                existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                rca_timestamp,
            ),
            "total_pipeline_time_ms": existing.get("total_pipeline_time_ms"),
            "remediation_triggered_at": existing.get("remediation_triggered_at"),
            "status": existing.get("status", "active"),
        }

    def _sync_incident_timings(self, hypotheses: list[Hypothesis], evaluated_at: str) -> None:
        active_ids: set[str] = set()
        for hypothesis in hypotheses:
            incident_id = self._incident_id(hypothesis)
            active_ids.add(incident_id)
            existing = self._incident_timings.get(incident_id, {})
            rca_timestamp = existing.get("rca_computation_timestamp", evaluated_at)
            self._incident_timings[incident_id] = {
                "event_ingestion_timestamp": existing.get("event_ingestion_timestamp", hypothesis.first_ingestion_timestamp),
                "anomaly_detection_timestamp": existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                "rca_computation_timestamp": rca_timestamp,
                "detection_to_rca_time_ms": self._duration_ms(
                    existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                    rca_timestamp,
                ),
                "remediation_triggered_at": existing.get("remediation_triggered_at"),
                "total_pipeline_time_ms": existing.get("total_pipeline_time_ms"),
                "status": "completed" if existing.get("remediation_triggered_at") else "active",
                "root_service": hypothesis.service,
            }
        with self._lock:
            self._active_incident_ids = active_ids

    def _trigger_remediation_if_needed(self, result: dict[str, Any]) -> str | None:
        incident_id = result.get("incident_id")
        if not incident_id:
            return result.get("timings", {}).get("remediation_triggered_at")
        if result.get("status") == "SUPPRESSED":
            return None
        if incident_id in self._triggered_incidents:
            return self._triggered_incidents[incident_id]
        trigger_timestamp = utc_timestamp_ms()
        trigger_payload = {
            "incident_id": incident_id,
            "timestamp": trigger_timestamp,
            "service": SERVICE_NAME,
            "root_causes": result.get("primary_root_cause", []),
            "status": result.get("status"),
            "confidence": result.get("confidence"),
            "affected_services": result.get("affected_services", []),
        }
        try:
            self.backend.publish(REMEDIATION_TOPIC, trigger_payload)
            self.logger.info(
                json.dumps(
                    {
                        "stage": "remediation_triggered",
                        "incident_id": incident_id,
                        "payload": trigger_payload,
                    },
                    ensure_ascii=True,
                )
            )
        except Exception:
            return None
        self._last_triggered_incident_id = incident_id
        self._triggered_incidents[incident_id] = trigger_timestamp
        if incident_id in self._incident_timings:
            self._incident_timings[incident_id]["remediation_triggered_at"] = trigger_timestamp
            self._incident_timings[incident_id]["total_pipeline_time_ms"] = self._duration_ms(
                self._incident_timings[incident_id].get("event_ingestion_timestamp"),
                trigger_timestamp,
            )
            self._incident_timings[incident_id]["status"] = "completed"
        return trigger_timestamp

    def _log_suppressed_event(self, result: dict[str, Any]) -> None:
        self.logger.info(
            json.dumps(
                {
                    "stage": "suppressed_event",
                    "incident_id": result.get("incident_id"),
                    "reason": result.get("reason", "Low confidence"),
                    "confidence": result.get("confidence"),
                    "threshold": CONFIDENCE_THRESHOLD,
                    "primary_root_cause": result.get("primary_root_cause", []),
                    "affected_services": result.get("affected_services", []),
                    "status": result.get("status"),
                },
                ensure_ascii=True,
            )
        )

    def _duration_ms(self, start_timestamp: str | None, end_timestamp: str | None) -> float | None:
        if not start_timestamp or not end_timestamp:
            return None
        return round(
            max(
                0.0,
                (parse_timestamp_to_epoch(end_timestamp) - parse_timestamp_to_epoch(start_timestamp)) * 1000.0,
            ),
            2,
        )

    def _record_latency_sample(self, result: dict[str, Any]) -> None:
        timings = result.get("timings", {})
        sample = {
            "incident_id": result.get("incident_id"),
            "event_ingestion_timestamp": timings.get("event_ingestion_timestamp"),
            "anomaly_detection_timestamp": timings.get("anomaly_detection_timestamp"),
            "rca_computation_timestamp": timings.get("rca_computation_timestamp"),
            "remediation_triggered_at": timings.get("remediation_triggered_at"),
            "detection_to_rca_time_ms": timings.get("detection_to_rca_time_ms"),
            "total_pipeline_time_ms": timings.get("total_pipeline_time_ms"),
            "status": timings.get("status", "active"),
        }
        with self._lock:
            if self._latency_history and self._latency_history[-1].get("incident_id") == sample["incident_id"]:
                self._latency_history[-1] = sample
            else:
                self._latency_history.append(sample)
        self.logger.info(
            json.dumps(
                {
                    "stage": "incident_latency",
                    "incident_id": sample["incident_id"],
                    "timings": sample,
                    "within_15s": bool(
                        sample["total_pipeline_time_ms"] is not None
                        and sample["total_pipeline_time_ms"] <= PIPELINE_SLA_MS
                    ),
                },
                ensure_ascii=True,
            )
        )

    def _relevant_services(self, latest: dict[str, Any]) -> list[str]:
        services: list[str] = []
        for field in (
            "primary_root_cause",
            "secondary_root_causes",
            "independent_failures",
            "affected_services",
            "missing_impact",
        ):
            for service in latest.get(field, []):
                if service not in services:
                    services.append(service)
        return services

    def _candidate_timeline_events(
        self,
        events: list[dict[str, Any]],
        relevant_services: list[str],
        incident_start_epoch: float,
        incident_end_epoch: float,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for event in events:
            if event.get("service") not in relevant_services:
                continue
            event_epoch = event.get("_event_epoch", 0.0)
            if event_epoch < incident_start_epoch or event_epoch > incident_end_epoch:
                continue
            if not self._is_relevant_timeline_event(event):
                continue
            candidates.append(event)
        candidates.sort(
            key=lambda item: (
                item.get("_event_epoch", 0.0),
                parse_timestamp_to_epoch(item.get("processing_timestamp")),
                item.get("service", ""),
                item.get("event_type", ""),
            )
        )
        return candidates

    def _is_relevant_timeline_event(self, event: dict[str, Any]) -> bool:
        if event.get("status") in {"FAILED", "DEGRADED"}:
            return True
        if event.get("event_type") == "ERROR":
            return True
        return float(event.get("latency") or 0.0) > 500.0

    def _timeline_summary(self, event: dict[str, Any], latest: dict[str, Any]) -> str:
        service = event.get("service")
        status = event.get("status")
        event_type = event.get("event_type")
        latency = float(event.get("latency") or 0.0)
        primary_roots = set(latest.get("primary_root_cause", []))
        affected = set(latest.get("affected_services", []))
        independent = set(latest.get("independent_failures", []))
        root_name = self._display_name(next(iter(primary_roots), service))

        if service == "database-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "Database failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "Database latency increased"
                return "Database degraded"
            if service in independent:
                return "Database failed independently"

        if service == "cache-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "Cache failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "Cache latency increased"
                return "Cache degraded"
            if service in affected:
                return f"Cache failed due to {root_name} issue"
            if service in independent:
                return "Cache failed independently"

        if service == "api-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "API failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "API response slowed down"
                return "API degraded"
            if service in affected:
                if "cache-service" in latest.get("affected_services", []) or "cache-service" in latest.get("causal_chain", [""])[0]:
                    return "API response slowed due to cache failure"
                return f"API response slowed due to {root_name} issue"
            if service in independent:
                return "API failed independently"

        if service in primary_roots:
            if event_type == "ERROR" or status == "FAILED":
                return f"{self._display_name(service)} failed"
            if latency > 500.0:
                return f"{self._display_name(service)} latency increased"
            return f"{self._display_name(service)} degraded"
        if service in independent:
            return f"{self._display_name(service)} failed independently"
        if service in affected:
            return f"{self._display_name(service)} was impacted by upstream issue"
        if status == "FAILED":
            return f"{self._display_name(service)} failed"
        if status == "DEGRADED":
            return f"{self._display_name(service)} degraded"
        if latency > 500.0:
            return f"{self._display_name(service)} latency increased"
        return f"{self._display_name(service)} had a relevant event"

    def _timeline_impact_level(self, service: str, latest: dict[str, Any]) -> str:
        if service in set(latest.get("primary_root_cause", [])):
            return "ROOT"
        causal_chain = latest.get("causal_chain", [])
        if not causal_chain:
            return "DOWNSTREAM"
        chain_services = [part.strip() for part in causal_chain[0].split("->")]
        if service in chain_services:
            index = chain_services.index(service)
            return "DOWNSTREAM" if index == 1 else "INDIRECT"
        return "DOWNSTREAM"

    def _display_name(self, service: str | None) -> str:
        if service == "database-service":
            return "Database"
        if service == "auth-service":
            return "Auth"
        if service == "payment-service":
            return "Payment"
        if service == "notification-service":
            return "Notification"
        if service == "cache-service":
            return "Cache"
        if service == "api-service":
            return "API"
        if not service:
            return "Service"
        return service.replace("-service", "").replace("-", " ").title()

    def _timeline_for(self, latest: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
        incident_id = latest.get("incident_id")
        if not incident_id:
            return {
                "incident_id": None,
                "window_seconds": WINDOW_SECONDS,
                "timeline": [],
            }

        relevant_services = self._relevant_services(latest)
        if not relevant_services:
            return {
                "incident_id": incident_id,
                "window_seconds": WINDOW_SECONDS,
                "timeline": [],
            }

        root_services = latest.get("primary_root_cause", [])
        root_timestamps = [
            parse_timestamp_to_epoch(event.get("timestamp"))
            for event in events
            if event.get("service") in root_services and event.get("status") in {"FAILED", "DEGRADED"}
        ]
        incident_end_epoch = parse_timestamp_to_epoch(latest.get("evaluated_at"))
        incident_start_epoch = min(root_timestamps) if root_timestamps else incident_end_epoch - WINDOW_SECONDS
        candidate_events = self._candidate_timeline_events(
            events,
            relevant_services,
            incident_start_epoch,
            incident_end_epoch,
        )

        timeline: list[dict[str, Any]] = []
        selected_services: set[str] = set()
        for event in candidate_events:
            service = event["service"]
            if service in selected_services:
                continue
            timeline.append(
                {
                    "time": event.get("timestamp"),
                    "service": service,
                    "message": self._timeline_summary(event, latest),
                    "root_cause": service in set(latest.get("primary_root_cause", [])),
                    "impact_level": self._timeline_impact_level(service, latest),
                    "event_type": event.get("event_type"),
                    "severity": event.get("status"),
                }
            )
            selected_services.add(service)

        timeline.sort(key=lambda item: (parse_timestamp_to_epoch(item.get("time")), item.get("service", "")))
        return {
            "incident_id": incident_id,
            "window_seconds": WINDOW_SECONDS,
            "causal_chain": latest.get("causal_chain", []),
            "timeline": timeline,
        }

    def rca_timeline(self) -> dict[str, Any]:
        with self._lock:
            latest = dict(self._latest)
            events = list(self._events)
        return self._timeline_for(latest, events)

    def latest(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._latest)

    def debug_latency(self) -> dict[str, Any]:
        with self._lock:
            incident_timings = {
                incident_id: dict(timings)
                for incident_id, timings in self._incident_timings.items()
            }
            active_incident_ids = set(self._active_incident_ids)
        samples = [
            {"incident_id": incident_id, **timings}
            for incident_id, timings in incident_timings.items()
        ]
        durations = [
            sample["total_pipeline_time_ms"]
            for sample in samples
            if sample.get("total_pipeline_time_ms") is not None
        ]
        detection_to_rca = [
            sample["detection_to_rca_time_ms"]
            for sample in samples
            if sample.get("detection_to_rca_time_ms") is not None
        ]
        avg_latency = round(sum(durations) / len(durations), 2) if durations else 0.0
        p95_latency = 0.0
        if durations:
            ordered = sorted(durations)
            index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
            p95_latency = round(ordered[index], 2)
        last_pipeline_time = durations[-1] if durations else 0.0
        last_detection_to_rca = detection_to_rca[-1] if detection_to_rca else 0.0
        return {
            "samples": len(durations),
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "last_pipeline_time_ms": last_pipeline_time,
            "last_detection_to_rca_time_ms": last_detection_to_rca,
            "active_incidents": len(active_incident_ids),
            "incident_breakdown": [
                {
                    "incident_id": sample["incident_id"],
                    "total_latency_ms": sample.get("total_pipeline_time_ms"),
                    "status": sample.get("status", "active"),
                }
                for sample in sorted(
                    samples,
                    key=lambda item: parse_timestamp_to_epoch(item.get("event_ingestion_timestamp")),
                )
            ],
            "within_15s": all(value <= PIPELINE_SLA_MS for value in durations) if durations else True,
        }


engine = RcaEngine()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "ok"}


@app.get("/rca/latest")
async def latest_rca() -> dict[str, Any]:
    return engine.latest()


@app.get("/rca/timeline")
async def rca_timeline() -> dict[str, Any]:
    return engine.rca_timeline()


@app.get("/incidents")
async def incidents() -> dict[str, Any]:
    history = engine.incident_history()
    return {
        "incidents": history,
        "incident_ids": [item.get("incident_id") for item in history if item.get("incident_id")],
    }


@app.get("/incidents/{incident_id}")
async def incident_by_id(incident_id: str) -> dict[str, Any]:
    incident = engine.get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return incident


@app.get("/incidents/{incident_id}/report")
async def incident_report(incident_id: str) -> dict[str, Any]:
    report = engine.get_incident_report(incident_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return report


def _safe_pdf_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).encode("latin-1", "replace").decode("latin-1")


def build_incident_report_pdf_bytes(report: dict[str, Any]) -> bytes:
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos

    class IncidentPDF(FPDF):
        def footer(self) -> None:
            self.set_y(-12)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(80, 80, 80)
            self.cell(0, 10, "AI Observability — Incident report", align="C")

    pdf = IncidentPDF()
    pdf.set_auto_page_break(auto=True, margin=16)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(20, 20, 20)
    pdf.cell(0, 10, "Incident report", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 6, "Structured summary for operators and stakeholders", ln=True)
    pdf.ln(6)

    def section_title(title: str) -> None:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(25, 25, 25)
        pdf.cell(0, 8, _safe_pdf_text(title), ln=True)
        pdf.set_draw_color(200, 200, 200)
        pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
        pdf.ln(3)
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(35, 35, 35)

    def paragraph(text: str) -> None:
        pdf.multi_cell(0, 6, _safe_pdf_text(text), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(1)

    section_title("Summary")
    paragraph(f"Incident ID: {report.get('incident_id', '')}")
    ts = report.get("timestamp")
    paragraph(f"Evaluated at: {ts if ts else 'Not recorded'}")
    paragraph(f"Status: {report.get('status', '')}")
    root_cause = report.get("root_cause")
    paragraph(f"Primary root cause (service): {root_cause if root_cause else 'Not determined'}")
    conf = report.get("confidence")
    try:
        c = float(conf)
        paragraph(f"Confidence score: {c * 100:.1f}%")
    except (TypeError, ValueError):
        paragraph(f"Confidence score: {conf}")
    paragraph(f"Outcome: {report.get('outcome', '')}")

    ce = report.get("confidence_explanation")
    if ce:
        section_title("Confidence explanation")
        paragraph(str(ce))

    chain = report.get("causal_chain") or []
    if chain:
        section_title("Causal chain")
        paragraph("Propagation path (upstream to downstream):")
        paragraph("  " + " -> ".join(_safe_pdf_text(x) for x in chain))

    reasoning = report.get("reasoning") or []
    if reasoning:
        section_title("Reasoning")
        for i, r in enumerate(reasoning, 1):
            paragraph(f"{i}. {_safe_pdf_text(r)}")

    ml = report.get("ml_summary") or {}
    section_title("ML anomaly signals")
    try:
        ls = float(ml.get("log_signal", 0) or 0)
        ms = float(ml.get("metric_signal", 0) or 0)
        tsig = float(ml.get("trace_signal", 0) or 0)
        paragraph(f"Log signal strength: {ls * 100:.1f}%")
        paragraph(f"Metric signal strength: {ms * 100:.1f}%")
        paragraph(f"Trace signal strength: {tsig * 100:.1f}%")
    except (TypeError, ValueError):
        paragraph(_safe_pdf_text(str(ml)))

    cb = report.get("confidence_breakdown") or {}
    if cb:
        section_title("Confidence breakdown")
        for k, v in cb.items():
            paragraph(f"{_safe_pdf_text(k)}: {_safe_pdf_text(v)}")

    affected = report.get("affected_services") or []
    if affected:
        section_title("Affected services")
        paragraph(", ".join(_safe_pdf_text(x) for x in affected))

    if report.get("action_taken"):
        section_title("Remediation action")
        paragraph(str(report.get("action_taken")))

    latent = report.get("latency_change") or {}
    if latent.get("before") or latent.get("after"):
        section_title("Pipeline timing")
        paragraph(f"Event ingestion: {latent.get('before')}")
        paragraph(f"Remediation triggered: {latent.get('after')}")

    suggestions = report.get("prevention_suggestions") or []
    if suggestions:
        section_title("Prevention suggestions")
        for i, s in enumerate(suggestions, 1):
            paragraph(f"{i}. {_safe_pdf_text(s)}")

    out = pdf.output()
    return bytes(out)


@app.get("/incidents/{incident_id}/download")
async def incident_download(incident_id: str):
    report = engine.get_incident_report(incident_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    from fastapi.responses import Response

    pdf_bytes = build_incident_report_pdf_bytes(report)
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in incident_id)[:120]
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="incident_report_{safe_name}.pdf"',
        },
    )


@app.get("/debug/latency")
async def debug_latency() -> dict[str, Any]:
    return engine.debug_latency()


@app.get("/export/prometheus", response_class=PlainTextResponse)
async def export_prometheus() -> PlainTextResponse:
    latency = engine.debug_latency()
    body = render_prometheus_metrics(
        SERVICE_NAME,
        [
            ("service_up", "Service availability state", "gauge", 1),
            ("rca_latency_samples", "Number of recorded RCA latency samples", "gauge", latency["samples"]),
            ("rca_avg_latency_ms", "Average RCA total pipeline latency in milliseconds", "gauge", latency["avg_latency_ms"]),
            ("rca_p95_latency_ms", "P95 RCA total pipeline latency in milliseconds", "gauge", latency["p95_latency_ms"]),
            (
                "rca_last_pipeline_time_ms",
                "Most recent RCA total pipeline latency in milliseconds",
                "gauge",
                latency["last_pipeline_time_ms"],
            ),
            (
                "rca_last_detection_to_rca_time_ms",
                "Most recent anomaly detection to RCA computation latency in milliseconds",
                "gauge",
                latency["last_detection_to_rca_time_ms"],
            ),
            ("rca_active_incidents", "Currently active incidents", "gauge", latency["active_incidents"]),
            ("rca_within_sla", "Whether all tracked incidents are within SLA", "gauge", latency["within_15s"]),
        ],
    )
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")
