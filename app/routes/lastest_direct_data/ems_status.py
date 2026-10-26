import json
import logging
from quart import Blueprint, request, jsonify
from pathlib import Path

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

ems_status_bp = Blueprint(
    'ems_status',
    __name__
)

EMS_Status_Path = Path(DataPaths.resolve("status/ems_status.json"))


@ems_status_bp.route(
    '/api/lastest-direct-data/ems-status',
    methods=['GET']
)
async def get_ems_status_data():
    try:
        if not EMS_Status_Path.exists():
            return jsonify({
                "success": False,
                "message": "EMS status file not found"
            }), 404

        with EMS_Status_Path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        return jsonify({
            "success": True,
            "data": data
        }), 200

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in EMS status file: {e}")
        return jsonify({
            "success": False,
            "message": "Invalid EMS status file format"
        }), 500

    except Exception as e:
        logger.exception("Failed to read EMS status file")
        return jsonify({
            "success": False,
            "message": "Internal server error",
            "error": str(e)
        }), 500
