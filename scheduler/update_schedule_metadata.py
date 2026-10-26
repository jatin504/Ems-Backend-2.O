import datetime
import json
import logging
import os
import asyncpg
from model.db import get_async_dict_cursor  # your existing function

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)


SCHEDULE_METADATA_FILE = DataPaths.resolve("metadata/metadata.json")
METADATA_DIR = SCHEDULE_METADATA_FILE.parent
os.makedirs(METADATA_DIR, exist_ok=True)


def floor_to_interval(dt: datetime.datetime, minutes=15):
    return dt.replace(minute=(dt.minute // minutes) * minutes, second=0, microsecond=0)


async def update_schedule_metadata():
    try:
        pool = await get_async_dict_cursor()
        async with pool.acquire() as db:
            rows = await db.fetch("""
                SELECT 
                    start_ts, end_ts, type, u_tariff, power, duration,
                    min_soc, max_soc, priority, remark, schedule_from
                FROM energy_strategy
                WHERE DATE(ts) = CURRENT_DATE
                ORDER BY start_ts ASC
            """)
    except Exception as e:
        logger.error(f"[Metadata Update] DB read error: {e}")
        return

    tariff_entries = []
    schedule_grouped = {}
    discharge_tariff_values = []

    # Build map of tariffs 
    for row in rows:
        if row["u_tariff"]:
            try:
                for item in row["u_tariff"].split(","):
                    ts = floor_to_interval(row["start_ts"])
                    value = float(item)
                    tariff_entries.append({
                        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "value": value
                    })
            except:
                pass

    # Prepare tariff map
    tariff_map = {
        datetime.datetime.strptime(entry["ts"], "%Y-%m-%d %H:%M:%S"): entry["value"]
        for entry in tariff_entries
    }

    # Process each schedule
    for row in rows:
        sched_type = row["type"].lower()

        if sched_type not in schedule_grouped:
            schedule_grouped[sched_type] = []

        start_ts = row["start_ts"]
        end_ts = row["end_ts"]

        # Only include tariff if u_tariff is present
        tariffs = None
        if row["u_tariff"]:
            try:
                tariffs = [float(x.strip()) for x in row["u_tariff"].split(",")]
            except:
                tariffs = None

        # Only include duration if provided
        durations = None
        if row["duration"]:
            try:
                durations = [int(x.strip()) for x in row["duration"].split(",")]
            except:
                durations = None

        # Only include power if provided
        power = None
        if row["power"] is not None:
            try:
                power = float(row["power"])
            except:
                power = None

        # Build schedule entry dynamically
        entry = {
            "startTs": start_ts.strftime("%Y-%m-%d %H:%M:%S")
        }

        if end_ts is not None:
            entry["endTs"] = end_ts.strftime("%Y-%m-%d %H:%M:%S")

        if tariffs:
            entry["tariff"] = tariffs

        if durations:
            entry["duration_minutes"] = durations

        if power is not None:
            entry["power"] = power

        # Add min/max SOC 
        if row["min_soc"] is not None: 
            entry["min_soc"] = float(row["min_soc"]) 

        if row["max_soc"] is not None: 
            entry["max_soc"] = float(row["max_soc"])

        # add the new fields
        optional_fields = ["priority", "remark", "schedule_from"]
        for f in optional_fields:
            if row.get(f) not in [None, ""]:
                entry[f] = row[f]

        schedule_grouped[sched_type].append(entry)

        # discharge tariff tracking
        if sched_type == "discharge" and tariffs:
            discharge_tariff_values.extend(tariffs)

    final_metadata = {
        "tariff": tariff_entries,
        "schedule": schedule_grouped,
        "discharge_tariff": discharge_tariff_values
    }

    # Write metadata.json
    try:
        with open(SCHEDULE_METADATA_FILE, "w") as f:
            json.dump(final_metadata, f, indent=2)
        logger.info("metadata.json updated successfully")
    except Exception as e:
        logger.error(f"Failed to write metadata.json: {e}")
