"""Version 2 sampled device API with optional tag selection."""
from quart import Blueprint, jsonify, make_response, request

from app.routes.lastest_direct_data.multiple_latest_devices import (
    get_multiple_latest_devices_data,
)
from utils.general_utils import parse_tags, select_tags

multiple_latest_devices_v2_bp = Blueprint("multiple_latest_devices_v2", __name__)


@multiple_latest_devices_v2_bp.route(
    "/lastest-direct-data/multiple-latest-devices", methods=["POST"]
)
async def get_multiple_latest_devices_data_v2():
    payload = await request.get_json()
    if not isinstance(payload, dict):
        return jsonify(success=False, message="Request body must be a JSON object"), 400
    try:
        tags = parse_tags(payload.get("tags"))
        if payload.get("device_ids") is not None and not isinstance(payload["device_ids"], str):
            raise ValueError("device_ids must be a comma-separated string")
        patterns = payload.get("device_id_patterns", [])
        if not isinstance(patterns, list) or any(not isinstance(p, str) for p in patterns):
            raise ValueError("device_id_patterns must be a list of strings")
    except ValueError as exc:
        return jsonify(success=False, message=str(exc)), 400

    # Reuse discovery, sampled resolution and per-device errors without changing v1.
    response = await make_response(await get_multiple_latest_devices_data())
    if response.status_code != 200:
        return response
    result = await response.get_json()
    result["data"] = {
        device_id: select_tags(data, tags)
        for device_id, data in result["data"].items()
    }
    return jsonify(result), 200
