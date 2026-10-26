from quart import request, jsonify, Blueprint
import json
import os
from collections import defaultdict

model_data_bp = Blueprint("model_data", __name__, url_prefix="/api/model")

# Absolute path to device_config.json
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

device_config = os.path.join(ROOT_DIR, "config", "device_config.json")
DEVICE_LIBRARY_DIR = os.path.join(ROOT_DIR, "device_library")

# GET: Return all files and folders in device_library in tree structure

@model_data_bp.route("/directory", methods=["GET"])
async def get_directory_tree_based_on_device_config_data():
    def build_tree():
        # read device_config file to get devices and computed_devices
        with open(device_config, "r") as f:
            config = json.load(f)
            devices = config.get("devices", [])
            computed_devices = config.get("computed_devices", [])
            
            tree = {"name": "Plant Architechture", "type": "directory", "children": []}

            grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            for device in devices + computed_devices:
                plant = f"plant_{device['plant']}" if device.get("plant") is not None else None
                block = f"block_{device['block']}" if device.get("block") is not None else None
                pcs = f"pcs_{device['pcs']}" if device.get("pcs") is not None else None
                device_type = device.get("deviceType", None)
                file_name = f"{device.get('device_id', 'unknown_id')}.json"
                grouped[plant or "_"][block or pcs or "_"][device_type or "_"].append(file_name)
            print("grouped is", dict(grouped))
            for plant, blocks_or_pcs in grouped.items():
                plant_node = {"name": plant, "type": "directory", "children": []} if plant != "_" else None
                for block_or_pcs, types in blocks_or_pcs.items():
                    block_or_pcs_node = {"name": block_or_pcs, "type": "directory", "children": []} if block_or_pcs != "_" else None
                    for device_type, files in types.items():
                        type_node = {"name": device_type, "type": "directory", "children": []} if device_type != "_" else None
                        # If device_type is "_", add files to block_or_pcs node
                        for file_name in files:
                            file_node = {
                                "name": file_name,
                                "path": os.path.join(plant, block_or_pcs, file_name),
                                "type": "file"
                            }
                            if type_node:
                                type_node["children"].append(file_node)
                            elif block_or_pcs_node:
                                block_or_pcs_node["children"].append(file_node)
                            elif plant_node:
                                plant_node["children"].append(file_node)
                            else:
                                tree["children"].append(file_node)
                
                        if type_node:
                            if block_or_pcs_node:
                                block_or_pcs_node["children"].append(type_node)
                            elif plant_node:
                                plant_node["children"].append(type_node)
                            else:
                                tree["children"].append(type_node)
                    if block_or_pcs_node:    
                        if plant_node:
                            plant_node["children"].append(block_or_pcs_node)
                        else:
                            tree["children"].append(block_or_pcs_node)
                if plant_node:
                    tree["children"].append(plant_node)
       
            return tree
    tree = build_tree()
    return jsonify(tree), 200   

# --------------------------

# based on device_id return data from device_library/device_type/make/model.json
@model_data_bp.route("/device_data/<device_id>", methods=["GET"])
async def get_model_data_based_on_device_id(device_id=None):
    if not device_id:
        return jsonify({"error": "Missing 'device_id' query parameter"}), 400

    try:
        with open(device_config, "r") as f:
            config = json.load(f)
            devices = config.get("devices", []) + config.get("computed_devices", [])
            device = next((d for d in devices if d.get("device_id") == device_id), None)
            if not device:
                return jsonify({"error": f"Device with id {device_id} not found"}), 404

            device_type = device.get("deviceType")
            make = device.get("make")
            model = device.get("model")

            if not all([device_type, make, model]):
                return jsonify({"error": f"Incomplete data for device {device_id}"}), 500

            file_path = os.path.join(DEVICE_LIBRARY_DIR, device_type, make, f"{model}.json")
            if not os.path.isfile(file_path):
                return jsonify({"error": f"File not found for device {device_id} at {file_path}"}), 404

            with open(file_path, "r") as df:
                data = json.load(df)

                return jsonify({
                    "result": data,
                    "make": make,
                    "model": model
                }), 200

    except Exception as e:
        return jsonify({"error": f"Failed to process request: {str(e)}"}), 500





# --------------------------
# GET: Return data from device_library/device_type/make/model.json
# --------------------------
@model_data_bp.route("/data/<filepath>", methods=["GET"])
async def get_model_data(filepath=None):
    print("file path is", filepath)
    if not filepath:
        return jsonify({"error": "Missing 'filepath' query parameter"}), 400

    # Ensure the requested file is within the device_library directory
    abs_path = os.path.abspath(filepath)
    if not abs_path.startswith(os.path.abspath(DEVICE_LIBRARY_DIR)):
        return jsonify({"error": "Invalid file path"}), 400

    if not os.path.isfile(abs_path):
        return jsonify({"error": f"File not found: {filepath}"}), 404

    try:
        with open(abs_path, "r") as f:
            data = json.load(f)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {str(e)}"}), 500
    