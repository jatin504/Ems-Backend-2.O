# dsm/schedule/scheduler.py
"""
Schedules rows from grid_req into energy_strategy table.
Idempotent: check existing energy_strategy rows and only insert new ones.


Assumes `energy_strategy` table exists and has at least columns:
start_ts, end_ts, type, power, schedule_from, remark, status


We mark inserted rows with schedule_from='EXTERNAL' and remark contains 'GRID REQUIREMENT'.
"""
from typing import List, Dict
from datetime import datetime
import asyncio


from model.db import get_async_connection

# Helper SQLs
CHECK_EXIST_SQL = """
SELECT id FROM energy_strategy
WHERE start_ts = $1 AND end_ts = $2 AND type = $3 AND power = $4 AND schedule_from = 'EXTERNAL'
LIMIT 1
"""


INSERT_SQL = """
INSERT INTO energy_strategy (start_ts, end_ts, type, tariff, power, duration, min_soc, max_soc, priority, schedule_from, remark, status, state)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'EXTERNAL', $10, $11, $12)
RETURNING id
"""


async def schedule_grid_reqs_to_energy_strategy(rows: List[Dict]) -> Dict:
    """Take rows (as returned by fetch_today_grid_reqs) and insert into energy_strategy idempotently.


    Args:
    rows: list of dicts {start_ts, end_ts, type, remark, power, unit}


    Returns:
    dict with keys: inserted_count, skipped_count, details
    """
    pool = await get_async_connection()
    inserted = 0
    skipped = 0
    details = []


    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                # normalize inputs
                start_ts = r.get("start_ts")
                end_ts = r.get("end_ts")
                typ = r.get("type")
                remark = r.get("remark") or "GRID REQUIREMENT"
                power = float(r.get("power") or 0.0)


                # check exists
                existing = await conn.fetchrow(CHECK_EXIST_SQL, start_ts, end_ts, typ, power)
                if existing:
                    skipped += 1
                    details.append({"start_ts": start_ts, "status": "skipped"})
                    continue

                # insert with defaults
                # Note: tariff/duration/min_soc/max_soc/priority/status/state left NULL or default; set sane defaults
                res = await conn.fetchrow(INSERT_SQL, start_ts, end_ts, typ, None, power, None, None, None, 'high', remark, 'PENDING', None)
                if res:
                    inserted += 1
                    details.append({"start_ts": start_ts, "status": "inserted", "id": res["id"] if "id" in res else None})
                else:
                    details.append({"start_ts": start_ts, "status": "failed"})

    await pool.close()
    return {"inserted": inserted, "skipped": skipped, "details": details}