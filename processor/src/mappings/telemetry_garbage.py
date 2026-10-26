from typing import Dict, Any
import json
import logging

from utils.db_utils import A_DB
from utils.db_write_queue import GARBAGE_QUEUE

logger = logging.getLogger(__name__)

class TelemetryGarbageDAO:

    @staticmethod
    async def insert(record):
        try:
            await GARBAGE_QUEUE.put(record)
        except Exception as e:
            logger.error(f"Error queueing garbage telemetry: {e}")


    @staticmethod
    async def previous_insert(record: Dict[str, Any]):
        try:
            query = """
            INSERT INTO telemetry_garbage (
                device_id,
                device_type,
                block,
                telemetry_alias,
                telemetry_name,
                raw_value,
                scaled_value,
                unit,
                min_value,
                max_value,
                reason,
                remarks,
                register,
                data_type,
                metadata
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15
            )
            """

            async with A_DB() as db:
                await db.execute(
                    query,
                    record.get("device_id"),
                    record.get("device_type"),
                    record.get("block"),
                    record.get("telemetry_alias"),
                    record.get("telemetry_name"),
                    record.get("raw_value"),
                    record.get("scaled_value"),
                    record.get("unit"),
                    record.get("min_value"),
                    record.get("max_value"),
                    record.get("reason"),
                    record.get("remarks"),
                    record.get("register"),
                    record.get("data_type"),
                    json.dumps(record.get("metadata", {}))
                )
        except Exception as e:
            logger.error(f"Error inserting garbage telemetry record: {e}")
            # raise RuntimeError("Failed to insert garbage telemetry records")

    @staticmethod
    async def bulk_insert(records: list[Dict[str, Any]]):

        try:

            if not records:
                logger.info("No garbage records to insert.")
                return

            query = """
            INSERT INTO telemetry_garbage (
                device_id, device_type, block,
                telemetry_alias, telemetry_name,
                raw_value, scaled_value, unit,
                min_value, max_value,
                reason, remarks,
                register, data_type, metadata
            )
            VALUES (
                $1,$2,$3,$4,$5,
                $6,$7,$8,$9,$10,
                $11,$12,$13,$14,$15
            )
            """

            values = [
                (
                    r.get("device_id"),
                    r.get("device_type"),
                    r.get("block"),
                    r.get("telemetry_alias"),
                    r.get("telemetry_name"),
                    r.get("raw_value"),
                    r.get("scaled_value"),
                    r.get("unit"),
                    r.get("min_value"),
                    r.get("max_value"),
                    r.get("reason"),
                    r.get("remarks"),
                    r.get("register"),
                    r.get("data_type"),
                    r.get("metadata", {})
                )
                for r in records
            ]

            async with A_DB() as db:
                await db.executemany(query, values)
        
            logger.info(f"Inserted {len(records)} garbage telemetry records.")

        except Exception as e:
            logger.error(f"Error inserting garbage telemetry records: {e}")
            raise RuntimeError("Failed to insert garbage telemetry records")
        
    @staticmethod
    async def garbage_worker():
        buffer = []

        while True:
            logger.info("TelemetryGarbageDAO garbage_worker waiting for items...")
            item = await GARBAGE_QUEUE.get()
            buffer.append(item)

            if len(buffer) >= 100:
                await TelemetryGarbageDAO.bulk_insert(buffer)
                buffer.clear()
                