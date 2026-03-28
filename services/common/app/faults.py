import os
import threading
import time

from fastapi import HTTPException


class FaultState:
    def __init__(self) -> None:
        self.extra_latency_ms = 0
        self.memory_leak_enabled = False
        self.memory_leak_chunks = 0
        self._memory_sink: list[bytes] = []
        self._lock = threading.Lock()

    def snapshot(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "extra_latency_ms": self.extra_latency_ms,
                "memory_leak_enabled": self.memory_leak_enabled,
                "memory_leak_chunks": self.memory_leak_chunks,
            }

    def apply_latency(self) -> None:
        if self.extra_latency_ms > 0:
            time.sleep(self.extra_latency_ms / 1000.0)

    def set_latency(self, extra_latency_ms: int) -> dict[str, int | bool]:
        with self._lock:
            self.extra_latency_ms = extra_latency_ms
            return self.snapshot()

    def set_memory_leak(self, enabled: bool, chunk_size_kb: int) -> dict[str, int | bool]:
        with self._lock:
            self.memory_leak_enabled = enabled
            if enabled:
                self._memory_sink.append(b"x" * chunk_size_kb * 1024)
                self.memory_leak_chunks = len(self._memory_sink)
            return self.snapshot()

    def tick_memory_leak(self) -> None:
        with self._lock:
            if self.memory_leak_enabled:
                self._memory_sink.append(b"x" * 256 * 1024)
                self.memory_leak_chunks = len(self._memory_sink)

    def reset(self) -> dict[str, int | bool]:
        with self._lock:
            self.extra_latency_ms = 0
            self.memory_leak_enabled = False
            self._memory_sink.clear()
            self.memory_leak_chunks = 0
            return self.snapshot()

    def crash(self) -> None:
        def _exit_soon() -> None:
            time.sleep(0.25)
            os._exit(1)

        thread = threading.Thread(target=_exit_soon, daemon=True)
        thread.start()


def ensure_upstream_success(status_code: int, detail: str) -> None:
    if status_code >= 500:
        raise HTTPException(status_code=502, detail=detail)
