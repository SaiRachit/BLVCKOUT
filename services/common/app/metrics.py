import statistics
import threading
import time
from typing import Any

from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.streaming import get_stream_publisher


class MetricsRegistry:
    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        self.started_at = time.time()
        self.request_count = 0
        self.error_count = 0
        self.latencies_ms: list[float] = []
        self._lock = threading.Lock()

    def record(self, latency_ms: float, success: bool) -> None:
        with self._lock:
            self.request_count += 1
            if not success:
                self.error_count += 1
            self.latencies_ms.append(latency_ms)
            if len(self.latencies_ms) > 512:
                self.latencies_ms = self.latencies_ms[-512:]

    def snapshot(self, trace_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            request_count = self.request_count
            error_count = self.error_count
            latencies = list(self.latencies_ms)

        avg_latency = round(statistics.fmean(latencies), 2) if latencies else 0.0
        if latencies:
            ordered = sorted(latencies)
            index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
            p95_latency = round(ordered[index], 2)
        else:
            p95_latency = 0.0

        error_rate = round(error_count / request_count, 4) if request_count else 0.0
        uptime_seconds = int(time.time() - self.started_at)
        return {
            "timestamp": utc_timestamp_ms(),
            "service": self.service_name,
            "trace_id": trace_id,
            "request_count": request_count,
            "error_count": error_count,
            "error_rate": error_rate,
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "uptime_seconds": uptime_seconds,
        }

    def emit_snapshot(self, trace_id: str | None = None) -> dict[str, Any]:
        snapshot = self.snapshot(trace_id=trace_id)
        publisher = get_stream_publisher()
        publisher.publish_metric(
            {
                **snapshot,
                "event_type": "METRIC_SNAPSHOT",
                "metric_name": "request_summary",
                "metric_value": snapshot["request_count"],
                "latency": snapshot["avg_latency_ms"],
            }
        )
        publisher.publish_metric(
            {
                "timestamp": snapshot["timestamp"],
                "service": self.service_name,
                "trace_id": trace_id,
                "event_type": "METRIC_POINT",
                "metric_name": "latency_ms",
                "metric_value": snapshot["avg_latency_ms"],
                "latency": snapshot["avg_latency_ms"],
                "error_rate": snapshot["error_rate"],
                "request_count": snapshot["request_count"],
                "error_count": snapshot["error_count"],
            }
        )
        publisher.publish_metric(
            {
                "timestamp": snapshot["timestamp"],
                "service": self.service_name,
                "trace_id": trace_id,
                "event_type": "METRIC_POINT",
                "metric_name": "error_rate",
                "metric_value": snapshot["error_rate"],
                "latency": snapshot["avg_latency_ms"],
                "request_count": snapshot["request_count"],
                "error_count": snapshot["error_count"],
            }
        )
        return snapshot


def render_prometheus_metrics(
    service_name: str,
    samples: list[tuple[str, str, str, float | int | bool | None]],
) -> str:
    lines: list[str] = []
    seen: set[str] = set()

    for metric_name, help_text, metric_type, value in samples:
        if value is None:
            continue
        if metric_name not in seen:
            lines.append(f"# HELP {metric_name} {help_text}")
            lines.append(f"# TYPE {metric_name} {metric_type}")
            seen.add(metric_name)
        numeric_value = int(value) if isinstance(value, bool) else value
        lines.append(f'{metric_name}{{service="{service_name}"}} {numeric_value}')

    return "\n".join(lines) + "\n"
