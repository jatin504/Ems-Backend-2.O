import asyncio
from collections import defaultdict

from utils.db_utils import A_DB
from utils.db_write_queue import DB_WRITE_QUEUE

from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
FLUSH_INTERVAL = 0.2  # seconds


class DBWriterWorkerResolutionSampling:

    def __init__(self):
        self.buffer = defaultdict(list)

    # -------------------------------
    # Normalization (REUSED LOGIC)
    # -------------------------------
    def _normalize_row(self, row: dict) -> dict:
        new_row = {}

        for k, v in row.items():
            if k == "time_stamp":
                if isinstance(v, str):
                    v = datetime.fromisoformat(v)
                elif not isinstance(v, datetime):
                    raise ValueError("Invalid timestamp type")

            elif k == "status":
                v = bool(v)

            elif isinstance(v, (int, float)):
                v = float(v)

            new_row[k] = v

        return new_row

    async def run(self):
        while True:
            try:
                items = await asyncio.wait_for(
                    DB_WRITE_QUEUE.get(),
                    timeout=FLUSH_INTERVAL
                )

                if isinstance(items, list):
                    affected_devices = set()

                    for device_id, row in items:
                        normalized = self._normalize_row(row)
                        self.buffer[device_id].append(normalized)
                        affected_devices.add(device_id)

                    for device_id in affected_devices:
                        if len(self.buffer[device_id]) >= BATCH_SIZE:
                            await self.flush_device(device_id)

                else:
                    device_id, row = items
                    normalized = self._normalize_row(row)
                    self.buffer[device_id].append(normalized)

                    if len(self.buffer[device_id]) >= BATCH_SIZE:
                        await self.flush_device(device_id)

            except asyncio.TimeoutError:
                for device_id in list(self.buffer.keys()):
                    await self.flush_device(device_id)
            except asyncio.CancelledError:
                logger.info("Worker stopping...")
                raise


    async def batch_run(self):
        while True:
            # logger.info("DB Writer Worker started...")
            try:
                # start_wait = time.monotonic()

                # Inside your DB Queue Consumer
                items = await DB_WRITE_QUEUE.get()
                if isinstance(items, list): # Handle the batch
                    for device_id, row in items:
                        # execute DB insert
                        normalized = self._normalize_row(row)
                        self.buffer[device_id].append(normalized)
                else: # Fallback for legacy single items
                    device_id, row = items
                    # execute DB insert
                    normalized = self._normalize_row(row)
                    self.buffer[device_id].append(normalized)

                device_id, row = await asyncio.wait_for(
                    DB_WRITE_QUEUE.get(),
                    timeout=FLUSH_INTERVAL
                )

                # wait_time = time.monotonic() - start_wait
                # if wait_time > 0.1:
                #     logger.warning(f"⚠ Queue wait time: {wait_time:.3f}s")

                # normalized = self._normalize_row(row)
                # self.buffer[device_id].append(normalized)

                if len(self.buffer[device_id]) >= BATCH_SIZE:
                    await self.flush_device(device_id)

            except asyncio.TimeoutError:
                for device_id in list(self.buffer.keys()):
                    await self.flush_device(device_id)

    async def flush_device(self, device_id):
        rows = self.buffer.pop(device_id, [])
        if not rows:
            return
        
        # start = time.monotonic()
        # row_count = len(rows)

        try:
            async with A_DB() as db:
                await self._bulk_insert(db, device_id, rows)
            
            # elapsed = time.monotonic() - start
            # rps = row_count / elapsed if elapsed > 0 else 0

            # logger.info(
            #     f"✅ DB flush | device={device_id} "
            #     f"rows={row_count} "
            #     f"time={elapsed:.3f}s "
            #     f"rate={rps:.0f} rows/s"
            # )
        except Exception as e:
            logger.error(f"❌ DB batch insert failed for {device_id}: {e}")


    async def _bulk_insert(self, db, device_id, rows):

        table = device_id.lower()

        columns = list(rows[0].keys())

        # Quote column names for safety
        col_sql = ", ".join([f'"{col}"' for col in columns])

        params = []
        values_sql = []

        idx = 1

        for row in rows:
            placeholders = []

            for col in columns:
                placeholders.append(f"${idx}")
                params.append(row.get(col))
                idx += 1

            values_sql.append(f"({', '.join(placeholders)})")

        # Exclude time_stamp from update clause
        update_columns = [
            col for col in columns
            if col != "time_stamp"
        ]

        update_sql = ", ".join([
            f'"{col}" = EXCLUDED."{col}"'
            for col in update_columns
        ])

        sql = f"""
            INSERT INTO {table} ({col_sql})
            VALUES {', '.join(values_sql)}

            ON CONFLICT (time_stamp)

            DO UPDATE SET
            {update_sql}
        """

        await db.execute(sql, *params)

        # logger.debug(
        #     f"📥 SQL executed | table={table} "
        #     f"rows={len(rows)} "
        #     f"sql_time={time.monotonic() - start:.3f}s"
        # )
