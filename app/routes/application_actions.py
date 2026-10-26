from quart import Blueprint, jsonify
import logging
import subprocess
import sys
from pathlib import Path

restart_bp = Blueprint("restart", __name__)


# Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("restart.log"),
#         logging.StreamHandler()
#     ]
# )
logger = logging.getLogger(__name__)

@restart_bp.route("/restart", methods=["POST"])
async def restart():
    try:
        # 🔐 Optional auth
        # token = request.headers.get("Authorization")
        # if token != "Bearer your-secret-token":
        #     return jsonify({"error": "Unauthorized"}), 401

        subprocess.Popen([sys.executable, "restart.py"])
        return {"message": "Restarting..."}

    except Exception as e:
        error_msg = f"💥 Failed to trigger restart: {str(e)}"
        logger.error(error_msg)
        return jsonify({"error": error_msg}), 500
