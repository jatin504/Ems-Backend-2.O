import os
import json
from quart import Blueprint, request, jsonify
from app.utils.rbac import require_permission

market_config_bp = Blueprint("market_config", __name__)

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

CONFIG_DIR = os.path.join(
    BASE_DIR,
    "controller_layer",
    "strategies",
    "trading_strategy",
    "config"
)

DAM_FILE = os.path.join(CONFIG_DIR, "dam_timing.json")
RTM_FILE = os.path.join(CONFIG_DIR, "rtm_timing.json")

DEFAULT_DAM = {
    "block_minutes": 0,
    "freeze_at_minutes": 0,
    "plan_start_minutes": 0,
    "plan_end_minutes": 0,
    "submit_at_minutes": 0
}

DEFAULT_RTM = {
    "block_minutes": 0,
    "freeze_at_minutes": 1,
    "plan_start_minutes": 3,
    "plan_end_minutes": 0,
    "submit_at_minutes": 0
}

def read_json(path, default_data):
    try:
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(default_data, f, indent=2)
            return default_data

        with open(path, "r") as f:
            return json.load(f)

    except Exception as e:
        raise Exception(f"Failed to read {path} : {str(e)}")


def write_json(path, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        raise Exception(f"Failed to write {path} : {str(e)}")


@market_config_bp.route("/market/dam-timing", methods=["GET"])
async def get_dam_timing():
    try:
        data = read_json(DAM_FILE, DEFAULT_DAM)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@market_config_bp.route("/market/dam-timing", methods=["POST"])
async def update_dam_timing():
    user, err = await require_permission("can_control_plant_config")
    if err:
        return err

    try:
        payload = await request.get_json()
        write_json(DAM_FILE, payload)
        return jsonify({"status": "success", "message": "DAM timing updated"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@market_config_bp.route("/market/rtm-timing", methods=["GET"])
async def get_rtm_timing():
    try:
        data = read_json(RTM_FILE, DEFAULT_RTM)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@market_config_bp.route("/market/rtm-timing", methods=["POST"])
async def update_rtm_timing():
    user, err = await require_permission("can_control_plant_config")
    if err:
        return err

    try:
        payload = await request.get_json()
        write_json(RTM_FILE, payload)
        return jsonify({"status": "success", "message": "RTM timing updated"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500