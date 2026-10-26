import json
import logging
from quart import Blueprint, request, jsonify
from pathlib import Path

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

multiple_latest_devices_bp = Blueprint(
    'multiple_latest_devices',
    __name__
)

PlantArchitecturePath = Path(DataPaths.resolve("plant_architecture"))


def _find_device_file(device_id: str) -> Path:
    """
    Search for a device file named {device_id}.json in the base directory
    and its subdirectories.
    """
    matches = list(PlantArchitecturePath.rglob(f"{device_id}.json"))
    if not matches:
        raise FileNotFoundError(f"No device file found for device ID: {device_id}")
    elif len(matches) > 1:
        raise ValueError(f"Multiple files found for device ID: {device_id}")
    return matches[0]


@multiple_latest_devices_bp.route(
    '/api/lastest-direct-data/multiple-latest-devices',
    methods=['POST']
)
async def get_multiple_latest_devices_data():
    try:
        payload = await request.get_json()

        # NEW: device_id pattern list
        device_id_patterns = payload.get("device_id_patterns", [])

        # OLD: direct device ids (still supported)
        device_ids_raw = payload.get("device_ids")

        device_ids = []

        # 1️⃣ explicit device_ids
        if device_ids_raw:
            device_ids.extend([
                d.strip() for d in device_ids_raw.split(",") if d.strip()
            ])

        # 2️⃣ pattern-based device discovery
        if device_id_patterns:
            for json_file in PlantArchitecturePath.rglob("*.json"):
                for pattern in device_id_patterns:
                    if pattern in json_file.name:
                        device_ids.append(json_file.stem)
                        break

        # remove duplicates
        device_ids = list(set(device_ids))

        if not device_ids:
            return jsonify({
                "success": False,
                "message": "No matching devices found"
            }), 400

        result = {}
        errors = {}

        for device_id in device_ids:
            try:
                device_file = _find_device_file(device_id)

                with device_file.open("r", encoding="utf-8") as f:
                    # FULL JSON (no key filtering)
                    result[device_id] = json.load(f)

            except Exception as e:
                logger.warning(f"Failed to load device {device_id}: {e}")
                errors[device_id] = str(e)

        return jsonify({
            "success": True,
            "data": result,
            "errors": errors
        }), 200

    except Exception as e:
        logger.exception("Unexpected error in multiple latest devices API")
        return jsonify({
            "success": False,
            "message": "Internal server error",
            "error": str(e)
        }), 500
