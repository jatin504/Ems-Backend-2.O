"""Restrict replicas to read handlers and expose bounded DB readiness."""
import asyncio
import logging
import os
from pathlib import Path

import asyncpg
from quart import g, jsonify, request

from api_scaling import replica_allowed

logger = logging.getLogger(__name__)


def configure_api_scaling(app):
    role = os.getenv("EMS_API_ROLE", "primary").lower()
    if role not in {"primary", "read"}:
        raise ValueError("EMS_API_ROLE must be primary or read")
    instance = os.getenv("EMS_API_INSTANCE", "api1")
    app.config["EMS_API_ROLE"] = role
    mutation_lock = asyncio.Lock()
    readiness_pool = None

    @app.before_serving
    async def start_readiness_pool():
        nonlocal readiness_pool
        # Reserve at most one connection per worker for probes. Request-pool
        # saturation must not evict a healthy worker from every proxy backend.
        # min_size=0 lets the API start while PostgreSQL is unavailable.
        readiness_pool = await asyncpg.create_pool(
            **app.db_config, min_size=0, max_size=1,
            timeout=3, command_timeout=3,
        )

    @app.after_serving
    async def stop_readiness_pool():
        if readiness_pool is not None:
            try:
                await asyncio.wait_for(readiness_pool.close(), timeout=2)
            except asyncio.TimeoutError:
                readiness_pool.terminate()

    @app.before_request
    async def restrict_replica():
        if role == "read" and not replica_allowed(request.method, request.path):
            return jsonify(error="This route requires the primary API"), 503
        # Serialize primary mutations across awaits: existing handlers can do a
        # read/modify/write in several steps. Internal SSE publishing is independent.
        mutation = request.method not in {"GET", "HEAD", "OPTIONS"}
        mutation = mutation and not replica_allowed(request.method, request.path)
        mutation = mutation and not request.path.startswith("/stream/internal/")
        # These GET handlers also write config or execute schedules.
        side_effect_get = request.path in {
            "/cycle-management/cycle-config", "/api/market/dam-timing",
            "/api/market/rtm-timing", "/api/load-schedule", "/api/load-and-schedule",
            "/api/forecast-schedule", "/api/load-forecast-schedule",
        }
        if role == "primary" and (mutation or side_effect_get):
            try:
                await asyncio.wait_for(mutation_lock.acquire(), timeout=10)
            except asyncio.TimeoutError:
                return jsonify(error="Primary API busy; request was not executed"), 503
            g.ems_mutation_lock = True

    @app.teardown_request
    async def release_mutation_lock(_error):
        if g.pop("ems_mutation_lock", False):
            mutation_lock.release()

    @app.after_request
    async def identify_instance(response):
        response.headers["X-EMS-Instance"] = instance
        return response

    @app.get("/health/ready")
    async def ready():
        try:
            # Shared data is intentionally kept on disk; don't silently create
            # a new data root when the intended folder/mount is unavailable.
            root = Path(os.environ["EMS_DATA_DIR"])
            if not root.is_dir() or not (root / "config/device_config.json").is_file():
                raise RuntimeError("Required EMS data/config is unavailable")

            async def check_db():
                if readiness_pool is None:
                    raise RuntimeError("Readiness pool has not started")
                async with readiness_pool.acquire(timeout=3) as connection:
                    await connection.fetchval("SELECT 1", timeout=3)

            await asyncio.wait_for(check_db(), timeout=3)
        except Exception:
            logger.warning("API readiness failed", exc_info=True)
            return jsonify(status="NOT_READY", instance=instance), 503
        return jsonify(status="READY", instance=instance, role=role), 200
