import json
import datetime
from pathlib import Path
import logging
import asyncio
import tempfile
from collections import defaultdict
import queue
import os

from threading import Lock

from runtime_paths import DataPaths
from utils.latest_telemetry_queue import LATEST_TELEMETRY_QUEUE

logger = logging.getLogger(__name__)

class LatestTelemetryManager:
    def __init__(self, base_dir="latest_telemetry"):
        self.base_dir = DataPaths.resolve(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

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
                    self._write_device_file(
                        device_id,
                        merged_data
                    )
                
                # await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Error While Updating Latest Telemetry: {e}")

    def _write_device_file(self, device_id: str, data: dict):
        file_path = self.base_dir / f"{device_id}.json"

        try:
            # load safely
            if file_path.exists():
                with open(file_path, "r") as f:
                    try:
                        existing_data = json.load(f).get("data", {})
                    except Exception:
                        existing_data = {}
            else:
                existing_data = {}

            existing_data.update(data)

            payload = {
                "time_stamp": datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "data": existing_data,
            }

            tmp_path = file_path.with_suffix(".tmp")

            with open(tmp_path, "w") as f:
                json.dump(payload, f, indent=2)

            tmp_path.replace(file_path)  # atomic replace

        except Exception as e:
            logger.error(f"Failed writing {device_id}: {e}")

    def ppsave_device_data(self, device_id: str, data: dict):
        file_path = self.base_dir / f"{device_id}.json"

        try:
            # Load existing data if file exists
            if file_path.exists():
                with open(file_path, "r") as f:
                    existing = json.load(f)
                    existing_data = existing.get("data", {})
            else:
                existing_data = {}

            # Merge new range data
            existing_data.update(data)

            data_to_write = {
                "time_stamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": existing_data,
            }

            with open(file_path, "w") as f:
                json.dump(data_to_write, f, indent=2)

        except Exception as e:
            logger.error(
                f"[LatestTelemetryManager] Failed to write {device_id}.json: {e}"
            )

    async def save_device_data(
        self,
        device_id: str,
        data: dict,
    ):
        lock = self._locks[device_id]

        async with lock:

            file_path = (
                self.base_dir
                / f"{device_id}.json"
            )

            try:
                # ---------------------------------------------------------
                # 1. Read existing JSON
                # ---------------------------------------------------------

                if file_path.exists():

                    try:
                        with open(
                            file_path,
                            "r",
                            encoding="utf-8",
                        ) as f:
                            existing = json.load(f)

                        existing_data = existing.get(
                            "data",
                            {},
                        )

                    except json.JSONDecodeError:
                        # Existing file is already corrupted.
                        #
                        # Don't overwrite blindly without logging it.
                        logger.error(
                            "[LatestTelemetryManager] "
                            "Corrupted JSON detected: %s",
                            file_path,
                        )

                        existing_data = {}

                else:
                    existing_data = {}

                # ---------------------------------------------------------
                # 2. Merge new telemetry
                # ---------------------------------------------------------

                existing_data.update(data)

                data_to_write = {
                    "time_stamp": (
                        datetime.datetime.now()
                        .strftime("%Y-%m-%d %H:%M:%S")
                    ),
                    "data": existing_data,
                }

                # ---------------------------------------------------------
                # 3. Atomic write
                # ---------------------------------------------------------
                #
                # NEVER directly do:
                #
                #   open(file_path, "w")
                #
                # because a crash/interruption can leave a partially
                # written JSON file.
                #
                # Instead:
                #
                #   write temporary file
                #          ↓
                #   flush + fsync
                #          ↓
                #   os.replace()
                #
                # os.replace() is atomic on the same filesystem.

                directory = file_path.parent

                fd, temp_path = tempfile.mkstemp(
                    dir=directory,
                    prefix=f".{device_id}.",
                    suffix=".tmp",
                )

                try:

                    with os.fdopen(
                        fd,
                        "w",
                        encoding="utf-8",
                    ) as f:

                        json.dump(
                            data_to_write,
                            f,
                            indent=2,
                        )

                        f.flush()
                        os.fsync(f.fileno())

                    os.replace(
                        temp_path,
                        file_path,
                    )

                except Exception:

                    try:
                        os.unlink(temp_path)
                    except FileNotFoundError:
                        pass

                    raise

            except Exception:
                logger.exception(
                    "[LatestTelemetryManager] "
                    "Failed to write %s.json",
                    device_id,
                )