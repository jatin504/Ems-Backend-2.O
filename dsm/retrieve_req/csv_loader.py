"""
Read CSV from path (env var GRID_REQ_CSV_PATH) and insert rows into `grid_req` table.
Idempotent: avoids duplicate inserts by using INSERT ... ON CONFLICT DO NOTHING.


CSV expected columns (header): start_ts,end_ts,type,remark,power,unit
start_ts/end_ts in ISO format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
"""
import os
import csv
from typing import Optional
from datetime import datetime
import asyncio


from dotenv import load_dotenv
from model.db import get_async_connection


load_dotenv() # load .env


GRID_REQ_CSV_PATH = os.getenv("GRID_REQ_CSV_PATH")


# NOTE: For ON CONFLICT to work you must have a unique constraint on grid_req, e.g.:
# CREATE UNIQUE INDEX IF NOT EXISTS grid_req_unique ON grid_req (start_ts, end_ts, type, power, unit);


async def load_csv_to_grid_req(csv_path: Optional[str] = None) -> int:
    """Load CSV into grid_req table. Returns number of inserted rows.


    Args:
    csv_path: optional override; if not provided uses GRID_REQ_CSV_PATH env var.
    """
    path = csv_path or GRID_REQ_CSV_PATH
    if not path:
        raise ValueError("CSV path not provided and GRID_REQ_CSV_PATH not set in env")


    inserted = 0
    pool = await get_async_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            with open(path, "r", newline="") as fh:
                rdr = csv.DictReader(fh)
                rows_to_insert = []
                for row in rdr:
                    # parse timestamps
                    def parse_dt(v):
                        if not v:
                            return None
                        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
                            try:
                                return datetime.strptime(v, fmt)
                            except Exception:
                                continue
                        raise ValueError(f"Unrecognized datetime format: {v}")
                    start_ts = parse_dt(row.get("start_ts"))
                    end_ts = parse_dt(row.get("end_ts"))
                    typ = row.get("type")
                    remark = row.get("remark")
                    power = float(row.get("power") or 0.0)
                    unit = row.get("unit") or "MW"
                    rows_to_insert.append((start_ts, end_ts, typ, remark, power, unit))
                if not rows_to_insert:
                    await pool.close()
                    return 0


                # Do upsert using ON CONFLICT DO NOTHING; requires unique index as noted above
                insert_sql = """
                INSERT INTO grid_req (start_ts, end_ts, type, remark, power, unit)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT DO NOTHING
                """
                for r in rows_to_insert:
                    await conn.execute(insert_sql, *r)
                    inserted += 1
    await pool.close()
    return inserted