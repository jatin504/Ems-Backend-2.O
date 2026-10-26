from quart import Blueprint, jsonify
import socket
from datetime import datetime

health_bp = Blueprint("health", __name__)

@health_bp.route("/health", methods=["GET"])
async def health():

    return jsonify({
        "status": "UP",
        "server": socket.gethostname(),
        "time": datetime.utcnow().isoformat()
    }), 200