from zoneinfo import ZoneInfo

import aiohttp
import asyncio
import time
import logging
import os
from datetime import datetime

from utils.db_config import db_config
from utils.db_write_queue import VICTORIA_DB_WRITE_QUEUE

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

VICTORIA_INSERT_URL = f"http://{os.getenv('PRIMARY_BACKEND_HOST')}:8428/api/v1/import/prometheus"

DB_NAME = db_config("primary")["database"]

class VictoriaWriter:

    def __init__(self):
        self.batch = []
        self.batch_size = 50_000

        self.flush_interval = 1  # seconds

        self.session = None
        self.flush_lock = asyncio.Lock()

    # -------------------------------------------------
    # START
    # -------------------------------------------------
    async def start(self):
        self.session = aiohttp.ClientSession()

        asyncio.create_task(self._flush_timer())

        while True:
            records = await VICTORIA_DB_WRITE_QUEUE.get()

            try:
                self._process_records(records)

                if len(self.batch) >= self.batch_size:
                    await self.flush()

            except Exception:
                logger.exception("Victoria queue processing error")

            finally:
                VICTORIA_DB_WRITE_QUEUE.task_done()

    # -------------------------------------------------
    # PROCESS RECORDS
    # -------------------------------------------------
    def _process_records(self, records):

        for device_id, mapped_values in records:

            timestamp = mapped_values.get("time_stamp")

            # fallback if missing
            if timestamp is None:
                timestamp = int(time.time() * 1000)

            # convert if string (old bug case)
            elif isinstance(timestamp, str):
                try:
                    # if someone passed ISO-like string (your current case)
                    # timestamp = int(
                    #     datetime.strptime(timestamp, "%Y-%m-%d %H:%M:00").timestamp() * 1000
                    # )
                    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:00")
                    dt = dt.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
                    timestamp = int(dt.timestamp() * 1000)
                except Exception:
                    # fallback to now if parsing fails
                    timestamp = int(time.time() * 1000)

            for alias, value in mapped_values.items():

                if alias == "time_stamp":
                    continue

                # Skip non-numeric safely
                if value is None:
                    continue  # safer than NaN for metrics

                if not isinstance(value, (int, float)):
                    continue

                metric_name = alias.lower()

                line = (
                    f'{metric_name}'
                    f'{{device="{device_id}",database="{DB_NAME}",resolution="1_min"}}'
                    f'{value} {timestamp}'
                )

                self.batch.append(line)

    # -------------------------------------------------
    # FLUSH TIMER
    # -------------------------------------------------
    async def _flush_timer(self):
        while True:
            await asyncio.sleep(self.flush_interval)

            try:
                await self.flush()
            except Exception:
                logger.exception("Victoria flush timer error")

    # -------------------------------------------------
    # FLUSH
    # -------------------------------------------------
    async def flush(self):

        async with self.flush_lock:

            if not self.batch:
                return

            payload = "\n".join(self.batch)

            try:
                async with self.session.post(
                    VICTORIA_INSERT_URL,
                    data=payload,
                    headers={
                        "Content-Type": "text/plain"
                    },
                ) as response:

                    if response.status != 204:
                        text = await response.text()
                        logger.error(
                            "Victoria write failed "
                            "status=%s response=%s",
                            response.status,
                            text
                        )
                    else:
                        logger.info(
                            "Victoria inserted %d metrics",
                            len(self.batch)
                        )

            except Exception:
                logger.exception("Victoria insert error")

            finally:
                self.batch.clear()

    # -------------------------------------------------
    # SHUTDOWN (optional but recommended)
    # -------------------------------------------------
    async def close(self):
        if self.session:
            await self.session.close()