import asyncio
from datetime import datetime, timezone
import json
import logging
import re
from threading import BoundedSemaphore

from quart import Blueprint, Response, current_app, jsonify, request

# Only publish approved messages and explicitly captured values. Paths, device
# connection details, exception text and other diagnostics stay in server logs.
LOG_SOURCES = {
    "schedule_strategy": (
        "controller_layer.strategies.classes.schedule_strategy",
        (
            # Rules are checked in order. Keep one rule per message because the first match wins.
            # \g<0> sends the full matched message as-is.
            (r"^Schedule Strategy Execution\.*$", r"\g<0>"),
            (r"^Schedule Strategy Execution", "Preparing schedule."),
            (r"^\[EVENT GENERATOR\] Fresh schedule written to ", "Schedule prepared."),
            (r"\[SIMULATION EVENT (\d{1,8})/(\d{1,8})\]", r"Executing event \1 of \2."),
            (r"^\[EVENT GENERATOR\] Event (SPE[0-9]{10}-[0-9]{2,8}\.[0-9]{2,8}) marked as running\b",
             r"Event \1 is running."),
            (r"Fault Code Detected: (\d{1,10})\.", r"Fault detected. Code \1."),
            (r"^\[EVENT GENERATOR\] Event (\d{1,8}) result saved to ", r"Event \1 result saved."),
            (r"^\[EVENT GENERATOR\] Completed (\d{1,8}) events\.", r"Completed \1 events."),
            (r"^Schedule strategy stopping\.", "Schedule strategy stopping."),
        ),
    ),
    "load_forecast": (
        "app.services.load_forecast_services",
        (
            (r"^Calling Forecasting Service at ", "Load forecast started."),
        ),
    ),
}


class LiveLogHandler(logging.Handler):
    def __init__(self, loop):
        super().__init__(level=logging.INFO)
        self.loop = loop
        self.subscribers = {}
        self.pending = BoundedSemaphore(256)
        self.sources = {logger: (source, rules) for source, (logger, rules) in LOG_SOURCES.items()}

    def emit(self, record):
        if record.name not in self.sources:
            return
        try:
            source, rules = self.sources[record.name]
            raw = record.getMessage()
            for pattern, replacement in rules:
                match = re.search(pattern, raw)
                if match:
                    message = match.expand(replacement)
                    break
            else:
                if record.levelno < logging.WARNING:
                    return
                message = "A warning or error occurred. See server logs."

            event = {
                "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                "source": source,
                "level": record.levelname,
                "message": message,
            }
        except Exception:
            self.handleError(record)
            return

        if self.pending.acquire(blocking=False):
            try:
                self.loop.call_soon_threadsafe(self.publish, event)
            except RuntimeError:  # The serving loop has shut down.
                self.pending.release()

    def publish(self, event):
        try:
            for queue, source in self.subscribers.items():
                if source and source != event["source"]:
                    continue
                if queue.full():
                    queue.get_nowait()
                queue.put_nowait(event)
        finally:
            self.pending.release()


live_logs_bp = Blueprint("live_logs", __name__)


async def serve_live_logs():
    handler = LiveLogHandler(asyncio.get_running_loop())
    current_app.extensions["live_logs"] = handler
    for logger in handler.sources:
        logging.getLogger(logger).addHandler(handler)
    try:
        yield
    finally:
        for logger in handler.sources:
            logging.getLogger(logger).removeHandler(handler)
        handler.close()
        current_app.extensions.pop("live_logs", None)


@live_logs_bp.get("/api/logs/stream")
async def stream_logs():
    handler = current_app.extensions["live_logs"]
    source = request.args.get("source")
    if source and source not in LOG_SOURCES:
        return jsonify({"error": "Unknown log source"}), 400

    async def events():
        queue = asyncio.Queue(maxsize=100)
        handler.subscribers[queue] = source
        try:
            yield ": connected\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                else:
                    yield f"event: log\ndata: {json.dumps(event)}\n\n"
        finally:
            handler.subscribers.pop(queue, None)

    response = Response(events(), mimetype="text/event-stream")
    response.timeout = None
    response.headers.update({
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Content-Encoding": "identity",
    })
    return response
