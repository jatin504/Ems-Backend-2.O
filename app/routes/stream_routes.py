from quart import Blueprint, Response, current_app, request, jsonify
import asyncio
import logging
import json

from utils.telemetry_store import TelemetryStore

logger = logging.getLogger(__name__)

stream_bp = Blueprint("stream_bp", __name__)

# REST API: Add internal publish endpoint
@stream_bp.route("/internal/publish", methods=["POST"])
async def publish_sse():
    # Security: Validate internal access
    api_key = request.headers.get("X-Internal-API-Key")
    if api_key != "y74y32bmcd": #os.getenv("INTERNAL_API_KEY")
        return jsonify({"error": "Unauthorized"}), 401

    try:
        payload = await request.json
        topic = payload.get("topic")
        data = payload.get("data")
    
        if not topic or data is None:
            return jsonify({"error": "Missing topic or data"}), 400

        broadcaster = current_app.broadcaster
        await broadcaster.broadcast(topic, data)
        return jsonify({"status": "published"}), 200
        
    except Exception as e:
        logger.error(f"Publish failed: {e}")
        return jsonify({"error": "Internal error"}), 500
    
# REST API: Support batch publish
@stream_bp.route("/internal/publish/batch", methods=["POST"])
async def publish_batch():
    payload = await request.json
    messages = payload.get("messages", [])  # [{"topic": "...", "data": {...}}, ...]
    
    broadcaster = current_app.broadcaster
    tasks = [
        broadcaster.broadcast(msg["topic"], msg["data"]) 
        for msg in messages if "topic" in msg and "data" in msg
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return jsonify({"published": len(tasks)}), 200

@stream_bp.route("", methods=["GET"])
async def stream():
    """Stream latest mapped snapshots for exact comma-separated device IDs.

    GET-only extension: both POST handlers above are unchanged. This is snapshot
    polling, not Valkey Pub/Sub. Both backends retain the original HTTP
    broadcaster messages alongside snapshots written by the mapping cache.
    """
    # Split ?topics=a,b, trim spaces, ignore blanks and deduplicate in input order.
    topics = list(dict.fromkeys(
        topic.strip() for topic in request.args.get("topics", "").split(",")
        if topic.strip()
    ))
    if not topics:
        return Response(status=400)

    # Capture the existing broadcaster before leaving the request context.
    broadcaster = current_app.broadcaster

    async def event_stream():
        """Poll selected snapshots and receive HTTP broadcasts for either backend."""
        # Capture .env backend settings for this stream. Mapped = instantaneous
        # named tags, not the minute-level sampled values used by the POST API.
        store = TelemetryStore("mapped")
        queues = []  # HTTP publisher subscriptions are independent of cache backend.
        last = {}  # Per-client duplicate suppression; NOT the persistent cache.
        try:
            # POST publishers use this local broadcaster for both JSON and Valkey.
            # Register before reading snapshots so updates are queued during reads.
            for topic in topics:
                queues.append(await broadcaster.register(topic))
            while True:
                sent = False
                for topic in topics:
                    try:
                        # read() selects JSON files or Valkey hashes from .env.
                        snapshot = await store.aread(topic)
                    except Exception:
                        logger.exception("Stream snapshot read failed for topic=%s", topic)
                        continue
                    # First available snapshot is sent immediately. Subsequently
                    # send changed tag dictionaries only; timestamps alone do not
                    # create duplicate events because read() returns tag data only.
                    if snapshot is not None and snapshot != last.get(topic):
                        last[topic] = snapshot
                        sent = True
                        # SSE wire format: data: <JSON> followed by a blank line.
                        # yield sends a chunk while keeping the connection open.
                        yield "data: " + json.dumps({"topic": topic, "message": snapshot}) + "\n\n"

                if queues:
                    # Preserve existing HTTP-published messages for either backend.
                    tasks = [asyncio.create_task(queue.get()) for queue in queues]
                    try:
                        # Wait for any topic, at most one second. This is a queue
                        # wait timeout, not an HTTP request/response timeout.
                        done, _ = await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED)
                        for index, task in enumerate(tasks):
                            if task not in done:
                                continue
                            topic = topics[index]
                            try:
                                # Broadcaster already formats "data: JSON\n\n".
                                # Strip the SSE prefix, then decode its JSON body.
                                message = json.loads(task.result()[5:].strip())
                            except (ValueError, TypeError):
                                logger.warning("Invalid broadcaster message for topic=%s", topic)
                                continue
                            if message != last.get(topic):
                                last[topic] = message
                                sent = True
                                yield "data: " + json.dumps({"topic": topic, "message": message}) + "\n\n"
                    finally:
                        # Cancel idle queue readers on timeout/disconnect, not just on messages.
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    # Valkey GET reads existing mapped hashes; no PUBLISH/SUBSCRIBE required.
                    await asyncio.sleep(1)
                if not sent:
                    # SSE comment: no new device data; not an error/data event.
                    yield ": keep-alive\n\n"
        finally:
            # A closed/cancelled browser request must not leave listeners/sockets.
            for topic, queue in zip(topics, queues):
                await broadcaster.unregister(topic, queue)
            if store.client is not None:
                await asyncio.to_thread(store.client.close)

    # Pass the generator itself: materializing its body would buffer forever.
    response = Response(event_stream(), mimetype="text/event-stream")
    # Disable Quart's response timer only; clients/proxies retain their own limits.
    response.timeout = None
    response.headers.update({
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
        "X-Accel-Buffering": "no",
        "Content-Encoding": "identity",
    })
    return response
