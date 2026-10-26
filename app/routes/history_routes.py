from quart import Blueprint, jsonify, request
from app.services.history_services import get_historical_data_from_devices
from device_config_helper import DeviceConfigHelper
import logging

logger = logging.getLogger(__name__)

history_bp = Blueprint("history_bp", __name__)


@history_bp.route("/<type>")
async def history(type):
    logger.info(f"Received request for historical data: {type}")

    # Parse query params
    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")
    keys_str = request.args.get("keys")

    logger.info(f"PARAMS : {start_ts, end_ts, keys_str} {request.args.to_dict(flat=False)}")

    # Validate required params
    if not start_ts or not end_ts:
        return jsonify({"error": "Missing startTs or endTs"}), 400

    # Get matching devices
    helper = DeviceConfigHelper()
    device_ids = helper.get_device_ids_by_type_pattern(type)

    if not device_ids:
        return jsonify([])

    # Fetch data
    result = await get_historical_data_from_devices(device_ids, start_ts, end_ts, keys_str)

    # Transform data
    structured_data = restructure_historical_data([dict(r) for r in result])

    # Convert asyncpg.Record to dict
    return structured_data



def restructure_historical_data(data):
    result = {}

    for row in data:
        table_name = row.get("table_name")
        timestamp = row.get("time_stamp")

        # Skip rows without required fields
        if not table_name or not timestamp:
            continue

        # Extract numeric suffix from table name: e.g. plant_1_pcs_1_ess_ess_2 → 2
        try:
            suffix = table_name.split("_")[-1]
            suffix_int = int(suffix)
        except (ValueError, IndexError):
            suffix_int = 1

        # Loop through all possible value columns
        for key, value in row.items():
            if key in ("table_name", "time_stamp"):
                continue

            if value is None:
                continue  # Skip null values if needed

            group_key = f"{key}-{suffix_int}"

            if group_key not in result:
                result[group_key] = []

            result[group_key].append({
                "ts": timestamp,
                "value": value,
                "tableName": table_name
            })

    return result