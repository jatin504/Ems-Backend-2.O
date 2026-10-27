"""Record explicitly selected, unaggregated engineering values in VictoriaMetrics."""

import asyncio
import json
import logging
import math
import os
import re
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
# A separate bounded queue prevents raw data from being interpreted as minute rows.
RAW_VM_QUEUE = asyncio.Queue(maxsize=10_000)


class RawRecorder:
    """Select exact aliases per device type/device ID; never auto-select every tag."""

    def __init__(self, policy=None, queue=None):
        self.queue = queue if queue is not None else RAW_VM_QUEUE
        self.enabled = os.getenv("EMS_RAW_VM_ENABLED", "false").lower() == "true"
        self.last_second = {}
        self.database = os.getenv("PRIMARY_DB_NAME", "ems")
        if not self.enabled and policy is None:
            self.policy = {}
            return
        # A configured path is relative to the repository, independent of EMS_DATA_DIR.
        path = Path(os.getenv("EMS_RAW_TAGS_FILE", "config/raw_historian_tags.json"))
        if not path.is_absolute():
            path = Path(__file__).resolve().parents[4] / path
        self.policy = policy if policy is not None else json.loads(path.read_text(encoding="utf-8-sig"))
        for section in ("device_types", "devices"):
            rules = self.policy.get(section, {})
            if not isinstance(rules, dict) or any(
                not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags)
                for tags in rules.values()
            ):
                raise ValueError(f"Raw historian {section} must map names to lists of exact aliases")
        logger.info("Raw historian policy loaded: %s", path)

    def record(self, device_id, mapped_values, timestamp_ms=None):
        """Queue at most one finite numeric sample per selected tag per second.

        Timestamp is backend receipt time: the current mapper has no source timestamp.
        Values are decoded/scaled engineering values, not encoded Modbus registers.
        """
        if not self.enabled:
            return
        now = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
        second = now // 1000
        lines, accepted = [], []
        for alias, meta in mapped_values.items():
            if not isinstance(meta, dict):
                continue
            # An explicit device rule overrides the type rule, including an empty list.
            tags = self.policy.get("devices", {}).get(device_id)
            if tags is None:
                tags = self.policy.get("device_types", {}).get(meta.get("device_type"), [])
            if alias not in tags or meta.get("type") == "alarm":
                continue
            value = meta.get("value")
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                continue
            identity = (device_id, alias)
            if second <= self.last_second.get(identity, -1):
                continue
            metric = alias.lower()
            if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", metric):
                logger.warning("Raw historian skipped invalid metric alias: %s", alias)
                continue
            # json.dumps escapes label quotes, newlines and backslashes correctly.
            labels = (f'device={json.dumps(device_id)},'
                      f'database={json.dumps(self.database)},'
                      'resolution="raw_1_sec"')
            lines.append(f"{metric}{{{labels}}} {float(value)} {now}")
            accepted.append(identity)
        if lines:
            try:
                self.queue.put_nowait(lines)
            except asyncio.QueueFull:
                # Prefer a visible data-loss warning over freezing device polling.
                logger.error("RAW DATA LOSS: queue full; dropped %d samples for %s", len(lines), device_id)
            else:
                for identity in accepted:
                    self.last_second[identity] = second


class RawVictoriaWriter:
    """Write raw batches independently, retaining the current batch on HTTP failure."""

    def __init__(self, queue=None, url=None):
        self.queue = queue if queue is not None else RAW_VM_QUEUE
        host = os.getenv("PRIMARY_BACKEND_HOST", "127.0.0.1")
        self.url = url or os.getenv("EMS_RAW_VM_URL", f"http://{host}:8428/api/v1/import/prometheus")

    async def send(self, session, lines):
        """Return success only on HTTP 204; caller retries the same timestamped data."""
        try:
            async with session.post(self.url, data="\n".join(lines),
                                    headers={"Content-Type": "text/plain"}) as response:
                if response.status == 204:
                    logger.info("Victoria RAW accepted %d samples", len(lines))
                    return True
                logger.error("Victoria RAW HTTP %s: %s", response.status, (await response.text())[:500])
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.exception("Victoria RAW unavailable; retaining batch for retry")
        return False

    async def start(self):
        """Drain bounded batches; retry outages without changing sampled-cache behavior."""
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                lines = list(await self.queue.get())
                count = 1
                # Coalesce queued device packets without waiting for a minute boundary.
                while len(lines) < 5000 and not self.queue.empty():
                    lines.extend(self.queue.get_nowait())
                    count += 1
                while not await self.send(session, lines):
                    await asyncio.sleep(2)
                for _ in range(count):
                    self.queue.task_done()
