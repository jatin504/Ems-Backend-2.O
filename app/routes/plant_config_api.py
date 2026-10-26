import os
import json
from quart import Blueprint, request, jsonify

from runtime_paths import DataPaths

plant_config_bp = Blueprint("plant_config", __name__)

CONFIG_PATH = DataPaths.resolve("config/plant_config.json")


#  SAVE / UPDATE API
@plant_config_bp.route("/save-plant-config", methods=["POST"])
async def save_plant_config():
    try:
        data = await request.get_json()

        plant_type = data.get("plant_type")
        plant_config = data.get("plant_config")

        # ---- read existing ----
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                try:
                    existing = json.load(f)
                except:
                    existing = {}
        else:
            existing = {}

        #  update only required keys
        if plant_type is not None:
            existing["plant_type"] = plant_type

        if plant_config is not None:
            existing["plant_config"] = plant_config

        # ---- write ----
        with open(CONFIG_PATH, "w") as f:
            json.dump(existing, f, indent=4)

        return jsonify({
            "success": True,
            "message": "Plant config updated successfully"
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


#  GET API (READ CONFIG)
@plant_config_bp.route("/get-plant-config", methods=["GET"])
async def get_plant_config():
    try:
        # ---- read full config file ----
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    return jsonify({
                        "success": False,
                        "message": f"Invalid JSON: {str(e)}"
                    }), 500
        else:
            return jsonify({
                "success": False,
                "message": "plant_config.json file not found"
            }), 404

        # ---- return full config exactly as stored ----
        return jsonify({
            "success": True,
            "data": data
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500