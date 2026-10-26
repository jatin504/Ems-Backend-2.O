from quart import Blueprint, Response, current_app, request, jsonify
import asyncio
import logging

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

@stream_bp.route("")
async def stream():
    topics = request.args.get("topics", "").split(",")
    if not topics or topics == [""]:
        logger.warning("No topics provided")
        return Response(status=400)

    logger.info(f"Received request for topics: {topics}")
    broadcaster = current_app.broadcaster
    queues = [await broadcaster.register(topic) for topic in topics]

    async def event_stream():
        try:
            while True:
                tasks = [asyncio.create_task(q.get(), name=f"SSE-{topic}") for q, topic in zip(queues, topics)]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=3.0)
                if not done:
                    logger.warning(f"No messages for topics {topics} after 3 seconds")
                    continue
                for task in done:
                    try:
                        message = task.result()
                        topic = topics[tasks.index(task)]
                        queue = queues[tasks.index(task)]
                        logger.debug(f"Sent message for topic '{topic}', queue size: {queue.qsize()}")
                        yield f"data: {{\"topic\": \"{topic}\", \"message\": {message[5:-2]}}}\n\n"
                    except Exception as e:
                        logger.error(f"Error processing message for topic '{topic}': {e}")
                for task in pending:
                    task.cancel()
        except asyncio.CancelledError:
            logger.info(f"Client disconnected from topics: {topics}")
            raise
        except Exception as e:
            logger.error(f"Stream error for topics {topics}: {e}")
        finally:
            for topic, queue in zip(topics, queues):
                await broadcaster.unregister(topic, queue)
                logger.info(f"Listener unregistered from topic: {topic}")

    response = Response(event_stream(), mimetype="text/event-stream")
    response.headers.update({
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*"
    })
    return response