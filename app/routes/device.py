from quart import request, jsonify, Blueprint, current_app
import json
import os
from auto_configs import PlantArchitect
from model import create_tables_from_json
from app.services.device_services import DeviceService

from runtime_paths import DataPaths
from utils.atomic_json import atomic_write_json

device = Blueprint("device", __name__, url_prefix="/api/device")

device_service = DeviceService()

# Absolute path to device_config.json
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# device_config = os.path.abspath(
#     os.path.join(BASE_DIR, "..", "..", "config", "device_config.json")
# )

CONFIG_PATH = "config/device_config.json"
device_config = DataPaths.resolve(CONFIG_PATH)

# --------------------------
# POST: Add new device
# --------------------------
@device.route("/", methods=["POST"])
async def add_device():
    body = await request.get_json()

    if not body or not isinstance(body, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    #Support both: "devices" and "device_data" (if it's a list)
    device_list = body.get("devices")
    if device_list is None:
        device_data = body.get("device_data")
        if device_data is None:
            return jsonify({"error": "Missing 'device_data' or 'devices'"}), 400

        #Allow device_data to be a list OR single object
        if isinstance(device_data, list):
            device_list = device_data
        else:
            device_list = [device_data]
    else:
        if not isinstance(device_list, list):
            return jsonify({"error": "Expected 'devices' to be a list"}), 400

    if len(device_list) == 0:
        return jsonify({"error": "Empty device list"}), 400

    # Read existing config
    try:
        with open(device_config, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {"devices": []}

    if "devices" not in config or not isinstance(config["devices"], list):
        config["devices"] = []

    # Build lookup map
    existing_devices = {
        dev["device_id"]: dev
        for dev in config["devices"]
        if isinstance(dev, dict) and "device_id" in dev
    }

    updated_count = 0
    new_count = 0

    for device in device_list:
        if not isinstance(device, dict):
            return jsonify({"error": "Each device must be an object"}), 400
        if "device_id" not in device:
            return jsonify({"error": "Missing 'device_id' in device"}), 400

        device_id = device["device_id"]

        if device_id in existing_devices:
            existing_devices[device_id].update(device)
            updated_count += 1
        else:
            existing_devices[device_id] = device
            new_count += 1

    # Save back
    config["devices"] = list(existing_devices.values())

    os.makedirs(os.path.dirname(device_config), exist_ok=True)
    atomic_write_json(device_config, config)

    architect = PlantArchitect(
        config_path="config/device_config.json",
        base_dir="plant_architecture",
    )
    architect.build_architecture()

    create_tables_from_json() 

    return jsonify({
        "message": "Devices upserted successfully",
        "total_devices": len(config["devices"]),
        "new": new_count,
        "updated": updated_count
    }), 200



# --------------------------
# GET: List all devices
# --------------------------
@device.route("/list", methods=["GET"])
async def list_devices():
    # import pdb; pdb.set_trace()   # Debugger stops here when GET request is called

    try:
        with open(device_config, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        return jsonify({"devices": []}), 200

    devices = config.get("devices", [])

    # Manual plant device for UI
    devices.append({
        "device_id": "plant_1_plant_device",
        "deviceType": "plant"
    })

    return jsonify({"devices": devices}), 200


@device.route("/device_types", methods=["GET"])
async def get_all_device_types():
    return jsonify(device_service.device_config_obj.get_all_device_types()), 200



@device.route("/device_columns/<device_ids>", methods=["GET"])
async def get_device_columns(device_ids):
    device_ids_arr = device_ids.split(",")
    columns = await device_service.get_device_columns(device_ids_arr)
    return jsonify({"columns": columns}), 200


@device.route("/data", methods=["POST"])
async def get_devices_data():
    data = await request.get_json()

    device_ids = data.get("deviceIds", [])
    params = data.get("params", [])
    dateType = data.get("dateType", "")
    frequency = data.get("frequency", "")
    interval = data.get("interval", "")

    # history specific
    startTs = data.get("startTs")
    endTs = data.get("endTs")

    try:
        device_data = await device_service.get_device_data(
            device_ids=device_ids,
            params=params,
            dateType=dateType,
            frequency=frequency,
            interval=interval,
            startTs=startTs,
            endTs=endTs,
        )
        return jsonify(device_data), 200

    except Exception as e:
        print(
            "Error fetching device data:------------------------------------------>",
            e,
        )
        return jsonify({
            "error": str(e)
        }), 500
