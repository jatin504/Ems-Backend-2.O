import time
import asyncio
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from plant_architecture_resolver import PlantArchitectureResolver
from ...computation import ComputationEngine
# from psycopg2 import sql
from model.db import get_connection, get_connection_center

from utils.db_utils import A_DB
from utils.db_write_queue import DB_WRITE_QUEUE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResolutionSampling:
    def __init__(self):
        self.tag_buffers = defaultdict(lambda: defaultdict(lambda: defaultdict(deque)))
        self.last_processed_time = None
        self.device_buffers = defaultdict(dict)
        self.tags_per_device = defaultdict(set)
        self.device_resolver = PlantArchitectureResolver()
        # self.compution = ComputationEngine()

        self.flush_running = False

        # Start RTM clock
        asyncio.create_task(
            self.rtm_clock()
        )

    async def rtm_clock(self):

        while True:

            try:

                now = datetime.now()

                seconds_to_next_minute = 60 - now.second

                await asyncio.sleep(seconds_to_next_minute)

                asyncio.create_task(
                    self.flush_all_devices()
                )

            except Exception as e:
                logger.exception(e)
            except asyncio.CancelledError:
                logger.info("Worker stopping...")
                raise

    async def add_data(self, device_id, tag, value, timestamp, resolution, aggregation):
        self.tag_buffers[device_id][resolution][tag].append((value, timestamp, aggregation))
        self.tags_per_device[device_id].add(tag)
        # logger.info(f"Added data to buffer | device: {device_id} | tag: {tag} | value: {value} | timestamp: {timestamp} | resolution: {resolution} | aggregation: {aggregation}")

    async def flush_all_devices(self):
        if self.flush_running:
            logger.warning(
                "Previous RTM flush still running. Skipping."
            )
            return

        self.flush_running = True

        logger.info(
                "⏰ Starting RTM flush... PPP"
            )

        try:
            now = datetime.now()

            # Round down to current minute
            timestamp_dt = (now.replace(second=0, microsecond=0))
            timestamp = int(timestamp_dt.timestamp())

            # Process devices
            tasks = [
                self._process_device(device_id, timestamp)
                for device_id in list(self.tag_buffers)
            ]

            await asyncio.gather(*tasks)
        
        except Exception as e:
            logger.info(f"Error while Flushing the devices: {e}")
        finally:
            self.flush_running = False

    async def _process_device(self, device_id, timestamp):

        try:

            # 1. SNAPSHOT + CLEAR must happen in event loop thread (safe zone)
            snapshot = self._snapshot_device_buffers(device_id)

            # 2. CPU aggregation moved to thread
            device_data, tag_count, sample_count = await asyncio.to_thread(
                self._aggregate_device_snapshot,
                snapshot
            )

            logger.info(
                "RTM DEVICE | device=%s | tags=%d | samples=%d | timestamp=%s",
                device_id,
                tag_count,
                sample_count,
                datetime.fromtimestamp(datetime.now().timestamp()) # timestamp | datetime.now().timestamp()
            )

            # 3. Async insert (non-blocking)
            if device_data:
                # logger.info(f"START {device_id}")

                await self._insert_device_data(
                    device_id,
                    timestamp,
                    device_data
                )

                # logger.info(f"END {device_id}")

        except Exception as e:
            logger.error(f"Error processing device {device_id}: {e}")

    def _snapshot_device_buffers(self, device_id):
        device_snapshot = {}

        device_buffers = self.tag_buffers.get(device_id, {})

        for resolution, tags in device_buffers.items():
            device_snapshot[resolution] = {}

            for tag, points in tags.items():

                if not points:
                    continue

                # COPY then CLEAR (atomic in event loop thread)
                device_snapshot[resolution][tag] = points.copy()
                points.clear()

        return device_snapshot
    
    def _aggregate_device_snapshot(self, snapshot):
        device_data = {}

        sample_count = 0
        tag_count = 0

        for resolution in snapshot:
            for tag, points in snapshot[resolution].items():

                if not points:
                    continue

                tag_count += 1

                aggregation = points[-1][2]
                values = [v for v, _, _ in points if v is not None]

                sample_count += len(values)

                if aggregation == "avg":
                    value = round(sum(values) / len(values), 2) if values else None

                elif aggregation == "last":
                    value = None
                    for v, _, _ in reversed(points):
                        if v is not None:
                            value = v
                            break

                else:
                    logger.warning(f"Unknown aggregation '{aggregation}' for tag '{tag}'")
                    value = None

                device_data[tag] = value

        return device_data, tag_count, sample_count

    async def _insert_device_data(self, device_id, timestamp, device_data):

        if not device_data:
            return

        # Convert RTM timestamp to formatted string
        time_stamp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

        # Determine app_status
        app_status = (
            500
            if all(value is None for value in device_data.values())
            else 200
        )

        # Build DB row
        row = {
            "time_stamp": time_stamp_str,
            "app_status": app_status,
        }

        row.update({tag.lower(): value for tag, value in device_data.items()})

        # Build update payload
        update_payload = {tag: {"value": value} for tag, value in device_data.items()}

        update_payload["Time_Stamp"] = {"value": time_stamp_str}
        update_payload["App_Status"] = {"value": app_status}

        # Push to plant architecture -- mandate 
        await self.device_resolver.previous_update_block_device(
            device_id=device_id, updates=update_payload
        )

        # Push to DB queue
        await DB_WRITE_QUEUE.put((device_id, row))
