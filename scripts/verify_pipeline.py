import json
import os
import sys
import time

os.environ["FORCE_INMEMORY_STREAM"] = "1"
os.environ["PLAIN_LOG_LEVEL"] = "ERROR"
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.event_processor_service.app.main import processor  # noqa: E402
from services.rca_service.app.main import engine  # noqa: E402
from services.synthetic_generator_service.app.main import generator  # noqa: E402


def main() -> None:
    time.sleep(3)
    payload = {
        "pipeline_health": processor.pipeline_health(),
        "stream_rate": processor.debug_stream_rate(),
        "window_events": processor.debug_window_events(seconds=3),
        "processing_latency": processor.debug_processing_latency(),
        "latest_rca": engine.latest(),
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    generator._running = False


if __name__ == "__main__":
    main()
