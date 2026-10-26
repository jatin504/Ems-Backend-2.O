from quart import Blueprint, jsonify, request
from app.services.history_services import get_historical_data_from_devices
import logging

logger = logging.getLogger(__name__)
target_id_key_history_bp = Blueprint("target_id_key_history_bp", __name__)


@target_id_key_history_bp.route("/", methods=["POST"])
async def history():
    logger.info("Received request for historical data (by device list)")

    # Parse query params
    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    if not start_ts or not end_ts:
        return jsonify({"error": "Missing startTs or endTs"}), 400

    # Parse JSON body
    body = await request.get_json()
    if not body or not isinstance(body, list):
        return jsonify({"error": "Request body must be a list of devices"}), 400

    # Group device IDs and keys
    device_map = {}
    for entry in body:
        device_id = entry.get("id")
        key = entry.get("key")
        if not device_id or not key:
            continue
        device_map.setdefault(device_id, []).append(key)

    # If nothing valid
    if not device_map:
        return jsonify([])

    all_results = []
    for device_id, keys in device_map.items():
        keys_str = ",".join(keys)
        result = await get_historical_data_from_devices([device_id], start_ts, end_ts, keys_str)
        all_results.extend([dict(r) for r in result])

    # Transform into frontend format
    structured_data = restructure_historical_data(all_results)
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