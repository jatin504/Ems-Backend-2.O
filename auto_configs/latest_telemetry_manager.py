import json
import logging
import asyncio
from collections import defaultdict
import queue

from threading import Lock

from utils.telemetry_store import TelemetryStore
from runtime_paths import DataPaths
from utils.latest_telemetry_queue import LATEST_TELEMETRY_QUEUE

logger = logging.getLogger(__name__)

class LatestTelemetryManager:
    def __init__(self, base_dir="latest_telemetry"):
        self.base_dir = DataPaths.resolve(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # One mapped-stage store is shared by this manager's save entry points.
        # "mapped" keeps these keys/files separate from sampled aggregates.
        # Backend settings are captured now; restart workers after .env changes.
        self.store = TelemetryStore("mapped", self.base_dir)
        self._latest_data = {}
        self._lock = Lock()
        self._locks = defaultdict(asyncio.Lock)

    def update_device_data(self, device_id: str, data: dict):
        """
        Store the latest telemetry for a device.
        """
        try:
            with self._lock:
                self._latest_data[device_id] = data
        except Exception as e:
            logger.error(
                f"Failed to update telemetry for {device_id}: {e}"
            )

    def get_device_data(self, device_id: str) -> dict | None:
        """
        Get the latest telemetry for a device.
        """
        with self._lock:
            return self._latest_data.get(device_id)

    def get_device_tag(
        self,
        device_id: str,
        tag: str,
        default=None,
    ):
        """
        Get a specific tag value from a device's latest telemetry.
        """
        with self._lock:
            device_data = self._latest_data.get(device_id)

        if device_data is None:
            return default

        return device_data.get(tag, default).get("value", default)

    def enqueue_device_data(self, device_id: str, data: dict):
        """
        Add telemetry update to queue.
        """
        try:
            LATEST_TELEMETRY_QUEUE.put(
                (device_id, data)
            )
        except Exception as e:
            logger.error(
                f"Failed to enqueue telemetry for {device_id}: {e}"
            )

    async def writer_loop(self, LATEST_TELEMETRY_QUEUE):
        """
        Runs forever.
        Every second:
            - Collect queued updates
            - Merge updates by device
            - Write once per device
        """
        logger.info("LatestTelemetry writer started")

        while True:
            await asyncio.sleep(1)


            queue_size = LATEST_TELEMETRY_QUEUE.qsize()
            # logger.info(f"Queue size: {queue_size}")

            updates = defaultdict(dict)

            try:

                for _ in range(queue_size):
                    try:
                        device_id, data = LATEST_TELEMETRY_QUEUE.get_nowait()

                        updates[device_id].update(data)

                        # LATEST_TELEMETRY_QUEUE.task_done()

                    except queue.Empty:
                        await asyncio.sleep(0.01)
                        break

                    except Exception as e:
                        logger.exception(f"Error while itterating Latest Telemetry Queue: {e}")

                # logger.info(f"Updates collected: {len(updates)}")
                
                if not updates:
                    continue

                for device_id, merged_data in updates.items():
                    await self.save_device_data(device_id, merged_data)
                
                # await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Error While Updating Latest Telemetry: {e}")

    def _write_device_file(self, device_id: str, data: dict):
        """Synchronous legacy writer; the method name no longer implies JSON-only.

        Used by older callers. merge() writes to the .env-selected backend and
        preserves unsupplied tags. It blocks its caller; async workers should use
        save_device_data() instead. Exceptions here propagate to the caller.
        """
        self.store.merge(device_id, data)

    def ppsave_device_data(self, device_id: str, data: dict):
        """Keep the older public method working with JSON and Valkey.

        This is another synchronous entry point, not a second/double write.
        It delegates once to the same mapped-stage merge implementation.
        """
        self.store.merge(device_id, data)

    async def save_device_data(self, device_id: str, data: dict):
        """Persist one device's latest mapped tags without blocking the event loop.

        PostMapper calls this method. amerge() runs synchronous file/network I/O
        in a worker thread; await waits for that attempt to finish. The backend
        is selected by self.store, so this function needs no JSON/Valkey branch.
        """
        # Do not stop the device batch when one snapshot write fails. Log the
        # traceback instead. There is no automatic retry, fallback or success flag;
        # a logged failure means this update is not guaranteed to have been saved.
        try:
            await self.store.amerge(device_id, data)
        except Exception:
            logger.exception("Mapped telemetry cache write failed for %s", device_id)
