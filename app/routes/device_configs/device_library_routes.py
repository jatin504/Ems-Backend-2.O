from quart import Blueprint, jsonify
import os

device_library = Blueprint("device_library", __name__, url_prefix="/device-library")

# Path to your device_library folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up 3 levels to reach backend/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", ".."))

# Now join with device_library folder
DEVICE_LIBRARY_DIR = os.path.join(ROOT_DIR, "device_library")

@device_library.route("/list", methods=["GET"])
async def list_device_library():
    result = {}

    # Walk through device_library
    for device_type in os.listdir(DEVICE_LIBRARY_DIR):
        type_path = os.path.join(DEVICE_LIBRARY_DIR, device_type)
        if not os.path.isdir(type_path):
            continue

        makes = []
        models = {}

        for make in os.listdir(type_path):
            make_path = os.path.join(type_path, make)
            if not os.path.isdir(make_path):
                continue

            makes.append(make)
            models[make] = []

            for model_file in os.listdir(make_path):
                if model_file.endswith(".json"):
                    model_name = os.path.splitext(model_file)[0]
                    models[make].append(model_name)

        result[device_type] = {"makes": makes, "models": models}

    return jsonify(result), 200
