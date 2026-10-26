from quart import Blueprint, request, jsonify
import json
from device_config_helper import DeviceConfigHelper
from app.utils.db_utils import DB

device_helper = DeviceConfigHelper()

dashboard_bp = Blueprint("dashboard_bp", __name__)


@dashboard_bp.route('/save', methods=['POST'])
async def save_dashboard():
    data = await request.get_json()

    # Validate required fields
    dashboard_id = data.get("id")
    title = data.get("title")
    configuration = data.get("config")

    if not (dashboard_id and title and configuration):
        return jsonify({"error": "Missing required fields"}), 400

    widgets = configuration.get("widgets", {})

    # Update each widget's "ids" array based on "patterns"
    for widget in widgets.values():
        try:
            devices = widget["dataSource"]["devices"]
            patterns = devices.get("patterns", [])
            resolved_ids = set()
            for pattern in patterns:
                matched_ids = device_helper.get_device_ids_by_type_pattern(pattern)
                resolved_ids.update(matched_ids)
            devices["ids"] = sorted(resolved_ids)
        except KeyError as e:
            return jsonify({"error": f"Malformed widget data: missing {str(e)}"}), 400

    # Serialize updated config
    updated_config_json = json.dumps(configuration)

    # Insert or update
    try:
        query = """
            INSERT INTO dashboards (id, title, config)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE
            SET title = EXCLUDED.title,
                config = EXCLUDED.config
        """
        async with DB() as db:
            await db.execute(query, dashboard_id, title, updated_config_json)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"status": "success", "id": dashboard_id}), 200


@dashboard_bp.route('/get', methods=['GET'])
async def get_dashboard():

    # Select
    try:
        query = """
            SELECT * FROM dashboards
        """
        async with DB() as db:
            result = await db.fetch(query)

        rows = [dict(record) for record in result]

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"result": rows}), 200