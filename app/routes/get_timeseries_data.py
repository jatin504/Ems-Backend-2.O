import asyncio
import json
import logging
from quart import Blueprint, request, jsonify
from app.utils.db_utils import DB
from app.services.table_record_transform.transforms import build_query, interval_to_metricsql, merge_results, resolve_time_range, timeseries_records_to_nested_dict, victoria_to_nested
from device_config_helper import DeviceConfigHelper

import aiohttp
import os

from dotenv import load_dotenv

from runtime.environment_const import TIMESERIES_DB

load_dotenv()


device_helper = DeviceConfigHelper()

logger = logging.getLogger(__name__)

timeseries_bp = Blueprint('timeseries', __name__)

VM_QUERY_URL = f"http://{os.getenv('PRIMARY_BACKEND_HOST')}:8428/api/v1/query_range"

@timeseries_bp.route('/api/timeseries-data', methods=['POST'])
async def get_timeseries_data():
    """
    POST /api/timeseries-data
    Body: {
        "dataSource": {
            "devices": {
                "patterns": ["_inverter_inverter_"],
                "ids": []  // will be auto-filled
            },
            "keys": [...],
            "aggregation": { ... },
            "timeRange": { ... },
            "orderBy": "DESC"
        }
    }
    """
    try:
        data = await request.get_json()
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        TSDB = TIMESERIES_DB

        if TSDB == "VICTORIA":
            print(f"VMMM")
            return await get_timeseries_data_by_victoria(data)
        else:
            print(f"PGGG")
            return await get_timeseries_data_by_postgres(data)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({
            "success": False,
            "error": "An unexpected error occurred"
        }), 500
    

async def get_timeseries_data_by_postgres(data):
    """
    POST /api/timeseries-data
    Body: {
        "dataSource": {
            "devices": {
                "patterns": ["_inverter_inverter_"],
                "ids": []  // will be auto-filled
            },
            "keys": [...],
            "aggregation": { ... },
            "timeRange": { ... },
            "orderBy": "DESC"
        }
    }
    """
    try:

        data_source = data.get("dataSource")
        if not data_source:
            return jsonify({"error": "dataSource is required in request body"}), 400

        # Resolve device IDs from patterns
        devices = data_source.get("devices", {})
        patterns = devices.get("patterns", [])
        resolved_ids = set()

        for pattern in patterns:
            matched_ids = device_helper.get_device_ids_by_type_pattern(pattern)
            if matched_ids:
                resolved_ids.update(matched_ids)

        # Update the config with resolved IDs
        if resolved_ids:
            devices["ids"] = sorted(resolved_ids, key=natural_sort_key)

        # Log for debugging
        print(f"Resolved device IDs: {devices['ids']} from patterns: {patterns}")

        # Execute PostgreSQL function
        async with DB() as db:
            try:
                # Call your PostgreSQL function
                # Assuming function name is "fn_get_timeseries_data"
                # t0 = time.perf_counter()
                result = await db.fetch(
                    "SELECT * FROM fn_get_timeseries_data($1::jsonb)",
                    json.dumps(data)  # Pass full enriched config
                )
                # t1 = time.perf_counter()

                # Transform records to nested dict
                transformed_data = timeseries_records_to_nested_dict(result)

                # t3 = time.perf_counter()

                # print(f"DB query time: {t1 - t0:.4f}s, Transformation time: {t3 - t1:.4f}s")

                return jsonify({
                    "success": True,
                    "data": transformed_data
                }), 200

            except Exception as db_error:
                logger.error(f"Database query failed: {db_error}")
                return jsonify({
                    "success": False,
                    "error": "Failed to fetch timeseries data from database",
                    "details": str(db_error) 
                }), 500

    except KeyError as ke:
        logger.warning(f"Malformed request: missing key {ke}")
        return jsonify({
            "success": False,
            "error": f"Malformed request data: missing {str(ke)}"
        }), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({
            "success": False,
            "error": "An unexpected error occurred"
        }), 500


async def get_timeseries_data_by_victoria(data):
    """
    POST /api/timeseries-data
    Body: {
        "dataSource": {
            "devices": {
                "patterns": ["_inverter_inverter_"],
                "ids": []  // will be auto-filled
            },
            "keys": [...],
            "aggregation": { ... },
            "timeRange": { ... },
            "orderBy": "DESC"
        }
    }
    """
    try:

        # print(f"Into Victoria Metrics Querying..")

        data_source = data.get("dataSource")
        if not data_source:
            return jsonify({"error": "dataSource is required in request body"}), 400

        queries = [
            {
                "metric": key["key"],
                "alias": key["alias"],
                "query": build_query(
                    metric=key["key"],
                    devices=data_source["devices"],
                    aggregation=data_source["aggregation"],
                ),
            }
            for key in data_source["keys"]
        ]

        start, end = resolve_time_range(data_source["timeRange"])

        func = data_source["aggregation"]["func"].upper()
        step = interval_to_metricsql(
            data_source["aggregation"]["interval"],
            data_source["aggregation"]["intervalType"],
        )
        if func == "NONE":
            step = "60s"

        # print(f"Build Query: {queries} for time range: {start} to {end}")

        async with aiohttp.ClientSession() as session:
            try:
                results = await asyncio.gather(
                    *(
                        fetch_query(session, q, start, end, "60s" if not step else step)
                        for q in queries
                    )
                )

                # print(f"Got results from VM..")

                nested_results = [
                    victoria_to_nested(r["result"], r["alias"])
                    for r in results
                ]

                # print(f"Transformed results to nested dicts..")

                final_data = merge_results(nested_results)

                # print(f"Final Merged Data !!")

                return jsonify({
                    "success": True,
                    "data": final_data
                }), 200

            except Exception as db_error:
                logger.error(f"Database query failed: {db_error}")
                return jsonify({
                    "success": False,
                    "error": "Failed to fetch timeseries data from database",
                    "details": str(db_error) 
                }), 500

    except KeyError as ke:
        logger.warning(f"Malformed request: missing key {ke}")
        return jsonify({
            "success": False,
            "error": f"Malformed request data: missing {str(ke)}"
        }), 400

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({
            "success": False,
            "error": "An unexpected error occurred"
        }), 500

async def fetch_query(session, query_info, start, end, step):
    params = {
        "query": query_info["query"],
        "start": start,
        "end": end,
        "step": step
    }

    async with session.post(VM_QUERY_URL, data=params) as resp:
        resp.raise_for_status()

        return {
            "alias": query_info["alias"],
            "metric": query_info["metric"],
            "result": await resp.json(),
        }

import re

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]