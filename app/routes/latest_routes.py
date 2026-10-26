from quart import Blueprint, request, jsonify
from device_config_helper import DeviceConfigHelper
from app.services.latest_services import get_latest_data_from_devices
import logging

logger = logging.getLogger(__name__)

latest_bp = Blueprint("latest_bp", __name__)


@latest_bp.route("/<type>")
async def latest(type):
    logger.info(f"Received request for type: {type}")

    # Get comma-separated key list from query string
    key_str = request.args.get("keys")

    helper = DeviceConfigHelper()
    ess_tables = helper.get_device_ids_by_type_pattern(type)

    logger.info(f"Filtered Tables are: {ess_tables}")

    data = await get_latest_data_from_devices(ess_tables, key_str)

    # Convert records to list of dicts (asyncpg.Record -> dict)
    result = [dict(row) for row in data]

    return jsonify(result)