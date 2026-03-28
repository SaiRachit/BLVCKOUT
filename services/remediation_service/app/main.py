import json
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from services.common.app.broker import get_stream_backend
from services.common.app.config import get_env
from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.metrics import render_prometheus_metrics
from services.common.app.plain_logging import get_plain_logger

SERVICE_NAME = get_env("SERVICE_NAME", "remediation-service")
REMEDIATION_TOPIC = get_env("REMEDIATION_TOPIC", "remediation_triggers_topic")
REMEDIATION_RESULTS_TOPIC = get_env("REMEDIATION_RESULTS_TOPIC", "remediation_results_topic")
CACHE_SERVICE_URL = get_env("CACHE_SERVICE_URL", "http://localhost:8001")
AUTH_SERVICE_URL = get_env("AUTH_SERVICE_URL", "http://localhost:8007")
DATABASE_SERVICE_URL = get_env("DATABASE_SERVICE_URL", "http://localhost:8002")
PAYMENT_SERVICE_URL = get_env("PAYMENT_SERVICE_URL", "http://localhost:8008")
NOTIFICATION_SERVICE_URL = get_env("NOTIFICATION_SERVICE_URL", "http://localhost:8009")
SYNTHETIC_GENERATOR_URL = get_env("SYNTHETIC_GENERATOR_URL", "http://localhost:8004")
REMEDIATION_COOLDOWN_SECONDS = float(get_env("REMEDIATION_COOLDOWN_SECONDS", "2"))


class RemediationEngine:
    def __init__(self) -> None:
        self.logger = get_plain_logger(SERVICE_NAME)
        self.backend = get_stream_backend()
        self._latest_action: dict[str, Any] = {
            "action": None,
            "status": "idle",
            "timestamp": None,
            "incident_id": None,
            "root_causes": [],
            "triggered_by": None,
            "reason": None,
            "expected_recovery": None,
            "cooldown_applied": False,
        }
        self._cooldowns: dict[str, float] = {}
        self._offsets = {REMEDIATION_TOPIC: "0-0"}
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self) -> None:
        while True:
            try:
                response = self.backend.read(self._offsets, block_ms=250, count=64)
                for topic, entries in response:
                    for entry_id, fields in entries:
                        self._offsets[topic] = entry_id
                        raw_payload = fields.get("payload")
                        if not raw_payload:
                            continue
                        payload = json.loads(raw_payload)
                        self._handle_trigger(payload)
            except Exception:
                time.sleep(0.5)

    def _handle_trigger(self, payload: dict[str, Any]) -> None:
        root_causes = payload.get("root_causes", [])
        if not root_causes:
            return
        primary = root_causes[0]
        decision = self._decide_action(primary)
        if not decision:
            return
        action_name, target_service = decision
        cooldown_key = f"{action_name}:{target_service}"
        now = time.time()
        if cooldown_key in self._cooldowns and now < self._cooldowns[cooldown_key]:
            result = {
                "action": action_name,
                "status": "cooldown_skipped",
                "timestamp": utc_timestamp_ms(),
                "incident_id": payload.get("incident_id"),
                "root_causes": root_causes,
                "target_service": target_service,
                "triggered_by": self._triggered_by(primary),
                "reason": "A recent remediation action already ran for the same service, so cooldown prevented a loop",
                "expected_recovery": self._expected_recovery(primary),
                "cooldown_applied": True,
            }
            self._store_result(result)
            return

        result = self._execute_action(
            action_name=action_name,
            target_service=target_service,
            payload=payload,
        )
        self._cooldowns[cooldown_key] = now + REMEDIATION_COOLDOWN_SECONDS
        self._store_result(result)

    def _decide_action(self, primary_root_cause: str) -> tuple[str, str] | None:
        if primary_root_cause == "database-service":
            return ("restart_database", "database-service")
        if primary_root_cause == "cache-service":
            return ("clear_cache", "cache-service")
        if primary_root_cause == "auth-service":
            return ("restart_auth", "auth-service")
        if primary_root_cause == "payment-service":
            return ("restart_payment", "payment-service")
        if primary_root_cause == "notification-service":
            return ("retry_queue", "notification-service")
        return (f"remediate_{primary_root_cause.replace('-', '_')}", primary_root_cause)

    def _execute_action(
        self,
        *,
        action_name: str,
        target_service: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        timestamp = utc_timestamp_ms()
        status = "success"
        details: dict[str, Any] = {}

        try:
            if action_name == "clear_cache":
                with httpx.Client(timeout=2.0) as client:
                    # 1. Clear actual cache data
                    response = client.post(f"{CACHE_SERVICE_URL}/internal/clear")
                    response.raise_for_status()
                    details["cache_clear"] = response.json()
                    # 2. Reset any injected faults
                    reset_resp = client.post(f"{CACHE_SERVICE_URL}/faults/reset")
                    details["fault_reset"] = reset_resp.json()
            elif action_name == "restart_database":
                with httpx.Client(timeout=2.0) as client:
                    response = client.post(f"{DATABASE_SERVICE_URL}/faults/reset")
                    response.raise_for_status()
                    details["fault_reset"] = response.json()
            elif action_name == "restart_auth":
                with httpx.Client(timeout=2.0) as client:
                    response = client.post(f"{AUTH_SERVICE_URL}/faults/reset")
                    response.raise_for_status()
                    details["fault_reset"] = response.json()
            elif action_name == "restart_payment":
                with httpx.Client(timeout=2.0) as client:
                    response = client.post(f"{PAYMENT_SERVICE_URL}/faults/reset")
                    response.raise_for_status()
                    details["fault_reset"] = response.json()
            elif action_name == "retry_queue":
                with httpx.Client(timeout=2.0) as client:
                    response = client.post(f"{NOTIFICATION_SERVICE_URL}/faults/reset")
                    response.raise_for_status()
                    details["fault_reset"] = response.json()
            else:
                details["info"] = f"Action {action_name} has no defined reset API, but generator sync will follow"
            
            # Closed-loop: notify generator to stop the failure
            try:
                with httpx.Client(timeout=2.0) as client:
                    gen_resp = client.post(f"{SYNTHETIC_GENERATOR_URL}/clear/{target_service}")
                    details["generator_sync"] = gen_resp.json()
            except Exception as gen_exc:
                details["generator_sync_error"] = str(gen_exc)
        except Exception as exc:
            status = "failed"
            details["error"] = str(exc)

        return {
            "action": action_name,
            "status": status,
            "timestamp": timestamp,
            "incident_id": payload.get("incident_id"),
            "root_causes": payload.get("root_causes", []),
            "target_service": target_service,
            "triggered_by": self._triggered_by(target_service),
            "reason": self._reason_for(target_service, payload),
            "expected_recovery": self._expected_recovery(target_service),
            "cooldown_applied": False,
            "details": details,
        }

    def _triggered_by(self, service_name: str) -> str:
        return f"{service_name} RCA"

    def _reason_for(self, service_name: str, payload: dict[str, Any]) -> str:
        status = payload.get("status")
        confidence = payload.get("confidence")
        if service_name == "database-service":
            if status == "CONFIDENT" and confidence is not None:
                return "High error rate and latency spike detected"
            return "Database instability detected by RCA"
        if service_name == "cache-service":
            if status == "CONFIDENT" and confidence is not None:
                return "Cache errors and latency degradation detected"
            return "Cache instability detected by RCA"
        if service_name == "auth-service":
            return "Authentication failures detected across login traffic"
        if service_name == "payment-service":
            return "Payment transaction instability detected"
        if service_name == "notification-service":
            return "Notification delivery failures detected"
        return "Service instability detected by RCA"

    def _expected_recovery(self, service_name: str) -> str:
        if service_name == "database-service":
            return "Cache and API performance should stabilize"
        if service_name == "cache-service":
            return "Cache hit rate should recover and API latency should improve"
        if service_name == "auth-service":
            return "Login traffic should recover once authentication stabilizes"
        if service_name == "payment-service":
            return "Purchase flow should stabilize after payment recovery"
        if service_name == "notification-service":
            return "Notification delivery backlog should clear after retries"
        return "Dependent services should stabilize if the remediation succeeds"

    def _store_result(self, result: dict[str, Any]) -> None:
        with self._lock:
            self._latest_action = result
        try:
            self.backend.publish(REMEDIATION_RESULTS_TOPIC, result)
        except Exception:
            pass
        self.logger.info(
            json.dumps(
                {
                    "stage": "remediation_action",
                    "result": result,
                },
                ensure_ascii=True,
            )
        )

    def latest(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._latest_action)


engine = RemediationEngine()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "ok"}


@app.get("/remediation/latest")
async def remediation_latest() -> dict[str, Any]:
    return engine.latest()


@app.get("/export/prometheus", response_class=PlainTextResponse)
async def export_prometheus() -> PlainTextResponse:
    latest = engine.latest()
    status_value = {
        "idle": 0,
        "success": 1,
        "cooldown_skipped": 0,
        "failed": -1,
    }.get(str(latest.get("status", "idle")).lower(), 0)
    body = render_prometheus_metrics(
        SERVICE_NAME,
        [
            ("service_up", "Service availability state", "gauge", 1),
            ("remediation_latest_status", "Latest remediation status encoded as numeric state", "gauge", status_value),
            ("remediation_cooldown_applied", "Whether the latest remediation was blocked by cooldown", "gauge", latest.get("cooldown_applied", False)),
        ],
    )
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")
