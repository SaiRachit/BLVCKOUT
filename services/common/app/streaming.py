import json
import logging
import queue
import threading
from typing import Any

from services.common.app.config import get_env
from services.common.app.config import get_stream_url
from services.common.app.broker import get_stream_backend


class StreamPublisher:
    def __init__(self, stream_url: str, logger: logging.Logger | None = None) -> None:
        self.stream_url = stream_url
        self.logger = logger
        self.logs_topic = get_env("LOGS_TOPIC", "logs_topic")
        self.metrics_topic = get_env("METRICS_TOPIC", "metrics_topic")
        self._queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(maxsize=4096)
        self._backend = get_stream_backend()
        self.backend_name = self._backend.backend_name
        self._worker = threading.Thread(target=self._drain_loop, daemon=True)
        self._worker.start()

    def publish_log(self, payload: dict[str, Any]) -> None:
        self._enqueue(self.logs_topic, payload)

    def publish_metric(self, payload: dict[str, Any]) -> None:
        self._enqueue(self.metrics_topic, payload)

    def _enqueue(self, topic: str, payload: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait((topic, payload))
        except queue.Full:
            if self.logger:
                self.logger.warning("stream publish dropped", extra={"extra": {"topic": topic}})

    def _drain_loop(self) -> None:
        while True:
            topic, payload = self._queue.get()
            try:
                stream_id = self._backend.publish(topic, payload)
                if self.logger:
                    self.logger.info(
                        "producer pushed event",
                        extra={
                            "service": payload.get("service", "stream-publisher"),
                            "trace_id": payload.get("trace_id"),
                            "event_type": payload.get("event_type", "STREAM_PUBLISH"),
                            "extra": {
                                "stream": topic,
                                "payload": payload,
                                "backend": self.backend_name,
                                "stream_id": stream_id,
                            },
                        },
                    )
            except Exception as exc:  # pragma: no cover - best effort transport
                if self.logger:
                    self.logger.warning(
                        "stream publish failed",
                        extra={"extra": {"topic": topic, "error": str(exc), "backend": self.backend_name}},
                    )
            finally:
                self._queue.task_done()


_publisher: StreamPublisher | None = None


def get_stream_publisher(logger: logging.Logger | None = None) -> StreamPublisher:
    global _publisher
    if _publisher is None:
        _publisher = StreamPublisher(get_stream_url(), logger=logger)
    return _publisher
