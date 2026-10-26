from app.utils.db_utils import DB
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def build_final_query(union_queries):
    if len(union_queries) == 1:
        full_query = union_queries[0]
    else:
        full_query = " UNION ALL ".join(union_queries)

    return f"""
        SELECT * FROM (
            {full_query}
        ) AS combined
        ORDER BY time_stamp ASC
    """


async def get_historical_data_from_devices(device_ids, start_ts, end_ts, keys):
    if not device_ids:
        return []

    # Sanitize keys
    keys = ('time_stamp,' + keys) if keys else '*'

    # Build per-table queries
    union_queries = []
    for idx, device_id in enumerate(device_ids):
        escaped_table = device_id.replace('"', '""')
        query = f"""
            (SELECT '{device_id}' AS table_name, {keys}
             FROM "{escaped_table}"
             WHERE time_stamp BETWEEN '{start_ts}' AND '{end_ts}')
        """
        union_queries.append(query)

    full_query = build_final_query(union_queries)

    # logger.info(f"FULL QUERY: {full_query}")

    async with DB() as db:
        result = await db.fetch(full_query)
        return result