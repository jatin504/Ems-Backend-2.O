"""
Functions to fetch today's grid requests from table `grid_req`.
Returns an array of dicts with fields: start_ts, end_ts, type, remark, power, unit.


Relies on your existing model/db.py async helpers.
"""
from typing import List, Dict
from datetime import datetime, date
import asyncio


from model.db import get_async_connection, get_async_dict_cursor


async def fetch_today_grid_reqs() -> List[Dict]:
    """Fetch rows from `grid_req` for today.


    Returns:
    List of dicts: {"start_ts": datetime, "end_ts": datetime, "type": str, "remark": str, "power": float, "unit": str}
    """
    pool = await get_async_dict_cursor()
    async with pool.acquire() as conn:
        # use record_class=DictRecord so rows act like dicts
        today = date.today()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())
        sql = """
        SELECT start_ts, end_ts, type, remark, power, unit
        FROM grid_req
        WHERE start_ts >= $1 AND start_ts <= $2
        ORDER BY start_ts
        """
        records = await conn.fetch(sql, start_of_day, end_of_day)
        results = []
        for r in records:
            results.append({
            "start_ts": r["start_ts"],
            "end_ts": r["end_ts"],
            "type": r["type"],
            "remark": r.get("remark"),
            "power": float(r.get("power") or 0.0),
            "unit": r.get("unit") or "MW",
            })
        await pool.close()
    return results