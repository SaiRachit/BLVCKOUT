import json
import threading
import time
from collections import defaultdict
from collections import deque
from typing import Any

import redis

from services.common.app.config import get_env
from services.common.app.config import get_stream_url


class InMemoryStreamBackend:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._topics: dict[str, deque[tuple[str, dict[str, str]]]] = defaultdict(deque)
        self._counters: dict[str, int] = defaultdict(int)

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        with self._lock:
            self._counters[topic] += 1
            stream_id = f"{self._counters[topic]}-0"
            self._topics[topic].append((stream_id, {"payload": json.dumps(payload, ensure_ascii=True)}))
            if len(self._topics[topic]) > 20000:
                self._topics[topic].popleft()
            return stream_id

    def read(self, offsets: dict[str, str], count: int = 100, block_ms: int = 250) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        deadline = time.time() + (block_ms / 1000.0)
        while True:
            result: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
            with self._lock:
                for topic, offset in offsets.items():
                    last_seen = int(offset.split("-")[0]) if offset else 0
                    entries = [(sid, data) for sid, data in self._topics[topic] if int(sid.split("-")[0]) > last_seen][:count]
                    if entries:
                        result.append((topic, entries))
            if result or time.time() >= deadline:
                return result
            time.sleep(0.01)

    def ping(self) -> bool:
        return True

    def recent_entries(self, topic: str, count: int = 100) -> list[tuple[str, dict[str, str]]]:
        with self._lock:
            entries = list(self._topics[topic])[-count:]
        return entries

    @property
    def backend_name(self) -> str:
        return "memory"


class RedisStreamBackend:
    def __init__(self, stream_url: str) -> None:
        self._client = redis.Redis.from_url(stream_url, decode_responses=True)
        self._client.ping()

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        return self._client.xadd(topic, {"payload": json.dumps(payload, ensure_ascii=True)}, maxlen=10000, approximate=True)

    def read(self, offsets: dict[str, str], count: int = 100, block_ms: int = 250) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        return self._client.xread(offsets, block=block_ms, count=count)

    def ping(self) -> bool:
        return bool(self._client.ping())

    def recent_entries(self, topic: str, count: int = 100) -> list[tuple[str, dict[str, str]]]:
        entries = self._client.xrevrange(topic, count=count)
        return list(reversed(entries))

    @property
    def backend_name(self) -> str:
        return "redis"


_backend_lock = threading.Lock()
_backend: RedisStreamBackend | InMemoryStreamBackend | None = None


def get_stream_backend() -> RedisStreamBackend | InMemoryStreamBackend:
    global _backend
    with _backend_lock:
        if _backend is not None:
            return _backend

        force_memory = get_env("FORCE_INMEMORY_STREAM", "0") == "1"
        if not force_memory:
            stream_url = get_stream_url()
            for attempt in range(10):
                try:
                    _backend = RedisStreamBackend(stream_url)
                    return _backend
                except Exception:
                    if attempt == 9:
                        break
                    time.sleep(0.5)

        _backend = InMemoryStreamBackend()
        return _backend
