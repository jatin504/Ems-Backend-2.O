from quart import Quart, request, jsonify, Blueprint
import json
import os

current_strategy = Blueprint("current_strategy", __name__)

STRATEGY_FILE = "controller_layer/current_strategy.json"

@current_strategy.route("/", methods=["GET"])
async def get_current_strategy():
    if not os.path.exists(STRATEGY_FILE):
        return jsonify({"error": "File not found"}), 404
    with open(STRATEGY_FILE, "r") as file:
        data = json.load(file)
    return jsonify(data)

@current_strategy.route("/", methods=["POST"])
async def set_current_strategy():
    body = await request.get_json()
    strategy = body.get("current_bess_strategy")
    if strategy is None:
        return jsonify({"error": "Missing 'current_bess_strategy'"}), 400

    with open(STRATEGY_FILE, "w") as file:
        json.dump({"current_bess_strategy": strategy}, file, indent=2)

    return jsonify({"message": "Strategy updated", "strategy": strategy})
