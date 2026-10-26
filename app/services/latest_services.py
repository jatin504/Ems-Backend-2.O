from app.utils.db_utils import DB
import logging
import asyncpg

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
        ORDER BY time_stamp DESC NULLS LAST
    """

async def get_latest_data_from_devices(device_ids, keys=None):
    if not device_ids:
        return []

    # Default columns if keys is None
    default_columns = [
        'time_stamp',
        'active_power_kw',
        'reactive_power_kvar',
        'charge_energy_kwh',
        'discharge_energy_kwh',
        'voltage',
        'amps'
    ]
    keys = ('time_stamp,' + keys) if keys else ','.join(default_columns)
    columns = [col.strip() for col in keys.split(',')]

    # Validate that time_stamp is included
    if 'time_stamp' not in columns:
        columns.insert(0, 'time_stamp')
        keys = ','.join(columns)

    union_queries = []
    for device_id in device_ids:
        escaped_table = device_id.replace('"', '""')
        
        # Main query with type casts
        main_select = ", ".join(
            ["time_stamp"] + [
                f"{col}::DOUBLE PRECISION" for col in columns if col != "time_stamp"
            ]
        )
        main_query = f"""
            -- {device_id}: Try real data
            (SELECT '{device_id}' AS table_name, {main_select}
             FROM "{escaped_table}"
             ORDER BY time_stamp DESC
             LIMIT 1)
        """
        
        # Fallback query with type casts
        fallback_select = ", ".join(
            ["NULL::TIMESTAMP AS time_stamp"] + [
                f"0.0::DOUBLE PRECISION AS {col}" for col in columns if col != "time_stamp"
            ]
        )
        fallback_query = f"""
            -- {device_id}: Fallback if table exists but empty
            (SELECT '{device_id}' AS table_name, {fallback_select}
             WHERE NOT EXISTS (
                 SELECT 1 FROM "{escaped_table}" LIMIT 1
             ))
        """
        
        # Combine main and fallback queries
        union_queries.append(f"{main_query} UNION ALL {fallback_query}")

    full_query = build_final_query(union_queries)

    logger.debug(f"Final Query:\n{full_query}")

    try:
        async with DB() as db:
            result = await db.fetch(full_query)
            # Convert asyncpg Record objects to dictionaries
            return [dict(row) for row in result]
    except asyncpg.UndefinedTableError as e:
        logger.error(f"Table does not exist: {e}")
        # Return null-filled rows for all devices
        result = []
        for device_id in device_ids:
            null_row = {'table_name': device_id}
            for col in columns:
                null_row[col] = None if col == 'time_stamp' else 0.0
            result.append(null_row)
        return result
    except asyncpg.UndefinedColumnError as e:
        logger.error(f"Column error: {e}")
        # Return null-filled rows for all devices
        result = []
        for device_id in device_ids:
            null_row = {'table_name': device_id}
            for col in columns:
                null_row[col] = None if col == 'time_stamp' else 0.0
            result.append(null_row)
        return result
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise  # Re-raise unexpected errors for debugging