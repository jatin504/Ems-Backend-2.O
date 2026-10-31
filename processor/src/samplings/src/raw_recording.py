"""Mapped readings -> awaited raw queue -> batched VictoriaMetrics HTTP writes."""
import asyncio
from dataclasses import dataclass
import json
import math
import os
import random
import time

import aiohttp

from utils.historian_common import record_month
from utils.raw_insertion_logging import raw_logger
from utils.raw_vm_queue import RAW_VM_QUEUE, RawPacket, positive_float, positive_int
from .raw_tag_selection import load_raw_tags


class RawRecorder:
    """Select raw tags and await queue space; perform no network or disk writes."""

    def __init__(self, queue=None, definitions_path=None):
        self.queue = queue if queue is not None else RAW_VM_QUEUE
        self.enabled = os.getenv("EMS_RAW_VM_ENABLED", "false").lower() == "true"
        self.selected = {}
        self.last_second = {}
        self.enqueued_samples = 0
        self._device_locks = {}
        self.packet_size = min(positive_int("EMS_RAW_PACKET_SAMPLES", 256),
                               positive_int("EMS_RAW_BATCH_SAMPLES", 5000))
        if self.enabled:
            self.logger = raw_logger()
            self.selected = load_raw_tags(definitions_path)
            self.logger.info("Raw selection loaded enabled_tags=%s queue_packets=%d packet_samples=%d",
                             {kind: len(tags) for kind, tags in self.selected.items() if tags},
                             self.queue.maxsize, self.packet_size)

    async def record(self, device_id, mapped_values, timestamp_ms=None):
        """Called by PostMapper.worker before the existing sampling enqueue.

        A full raw queue pauses this producer until space is available. Per-device
        locking keeps same-second deduplication correct across mapper workers.
        """
        if not self.enabled:
            return
        timestamp = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
        lock = self._device_locks.setdefault(device_id, asyncio.Lock())
        async with lock:
            samples = self._select_samples(device_id, mapped_values, timestamp // 1000)
            for offset in range(0, len(samples), self.packet_size):
                chunk = samples[offset:offset + self.packet_size]
                packet = RawPacket(str(device_id), timestamp,
                                   tuple((metric, value) for alias, metric, value in chunk))
                # Same contract as sampling_queue.put: wait, never drop on QueueFull.
                await self.queue.put(packet)
                self.enqueued_samples += len(chunk)
                for alias, metric, value in chunk:
                    self.last_second[(device_id, alias)] = timestamp // 1000

    def _select_samples(self, device_id, mapped_values, second):
        """Keep valid selected readings, at most one per device/tag/second."""
        samples = []
        for alias, metadata in mapped_values.items():
            if not isinstance(metadata, dict) or metadata.get("type") == "alarm":
                continue
            device_type = str(metadata.get("device_type", "")).lower()
            metric = self.selected.get(device_type, {}).get(alias)
            if metric is None or second <= self.last_second.get((device_id, alias), -1):
                continue
            value = metadata.get("value")
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                continue
            samples.append((alias, metric, value))
        return samples


@dataclass(frozen=True, slots=True)
class RawBatch:
    packets: list[RawPacket]
    sample_count: int
    completed_queue_items: int


class RawVictoriaWriter:
    """Consume raw packets, batch them, and acknowledge only after HTTP 204."""

    def __init__(self, queue=None, url=None):
        self.queue = queue if queue is not None else RAW_VM_QUEUE
        host = os.getenv("PRIMARY_BACKEND_HOST", "127.0.0.1")
        self.url = url or os.getenv("EMS_RAW_VM_URL", f"http://{host}:8428/api/v1/import/prometheus")
        self.database_label = json.dumps(os.getenv("PRIMARY_DB_NAME", "ems"))
        self.batch_size = positive_int("EMS_RAW_BATCH_SAMPLES", 5000)
        self.flush_seconds = positive_float("EMS_RAW_FLUSH_SECONDS", 1)
        self.log_seconds = positive_float("EMS_RAW_LOG_INTERVAL_SECONDS", 30)
        self.retry_seconds = positive_float("EMS_RAW_RETRY_SECONDS", 1)
        self.retry_max = max(self.retry_seconds, positive_float("EMS_RAW_RETRY_MAX_SECONDS", 30))
        self.logger = raw_logger()
        self.accepted_samples = self.accepted_batches = self.failures = 0
        self._last_summary = self._last_error = float("-inf")
        self._remaining_packet = None

    async def start(self):
        """One connection/session and one ordered writer for the raw queue."""
        self.logger.info("Raw writer started batch_samples=%d flush_seconds=%s endpoint=%s",
                         self.batch_size, self.flush_seconds, self.url)
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while True:
                batch = await self._collect_batch()
                payload = await asyncio.to_thread(self._encode_batch, batch.packets)
                await self._send_with_retry(session, payload)
                # A split queue item is acknowledged only after its final part.
                for _ in range(batch.completed_queue_items):
                    self.queue.task_done()
                self.accepted_samples += batch.sample_count
                self.accepted_batches += 1
                self._log_summary(batch.sample_count, len(payload))

    async def _collect_batch(self):
        """Stop collecting at the sample limit or the first-packet deadline."""
        packet = self._remaining_packet
        self._remaining_packet = None
        if packet is None:
            packet = await self.queue.get()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.flush_seconds
        packets, sample_count, completed_items = [], 0, 0
        while True:
            space = self.batch_size - sample_count
            if len(packet.samples) > space:
                packets.append(RawPacket(packet.device, packet.timestamp, packet.samples[:space]))
                self._remaining_packet = RawPacket(packet.device, packet.timestamp, packet.samples[space:])
                sample_count += space
                break
            packets.append(packet)
            sample_count += len(packet.samples)
            completed_items += 1
            remaining_seconds = deadline - loop.time()
            if sample_count == self.batch_size or remaining_seconds <= 0:
                break
            try:
                packet = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                try:
                    packet = await asyncio.wait_for(self.queue.get(), remaining_seconds)
                except asyncio.TimeoutError:
                    break
        return RawBatch(packets, sample_count, completed_items)

    def _encode_batch(self, packets):
        """Build VM import text off the event loop; called once per batch."""
        lines = []
        for packet in packets:
            labels = (f'device={json.dumps(packet.device)},database={self.database_label},'
                      f'resolution="raw_1_sec",record_month="{record_month(packet.timestamp)}"')
            lines.extend(f"{metric}{{{labels}}} {float(value)} {packet.timestamp}"
                         for metric, value in packet.samples)
        return "\n".join(lines).encode("utf-8")

    async def _send_with_retry(self, session, payload):
        """Hold this exact payload until accepted; do not dequeue another batch."""
        delay = self.retry_seconds
        while not await self.send(session, payload):
            await asyncio.sleep(delay * random.uniform(0.9, 1.1))
            delay = min(self.retry_max, delay * 2)

    async def send(self, session, payload):
        """HTTP acceptance is not proof that VM native retention kept the samples."""
        try:
            async with session.post(self.url, data=payload,
                                    headers={"Content-Type": "text/plain"}) as response:
                if response.status == 204:
                    return True
                reason = f"HTTP {response.status}: {(await response.text())[:500]}"
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            reason = str(error)
        self.failures += 1
        now = time.monotonic()
        if now - self._last_error >= self.log_seconds:
            self.logger.error("Raw VM unavailable; retrying same batch failures_total=%d reason=%s",
                              self.failures, reason)
            self._last_error = now
        return False

    def _log_summary(self, samples, payload_bytes):
        now = time.monotonic()
        if now - self._last_summary < self.log_seconds:
            return
        self.logger.info("Raw HTTP accepted samples_total=%d batches_total=%d last_batch=%d bytes=%d queue_packets=%d failures_total=%d",
                         self.accepted_samples, self.accepted_batches, samples, payload_bytes,
                         self.queue.qsize(), self.failures)
        self._last_summary = now
