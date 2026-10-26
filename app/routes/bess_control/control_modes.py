from quart import Blueprint, jsonify
import json
from pathlib import Path

from .init_bp import bess_control_bp

from runtime_paths import DataPaths

@bess_control_bp.route("/set_bess_control_modes", methods=["POST"])
async def set_bess_control_modes():
    """
    Endpoint to set BESS control modes.
    Expects JSON payload with mode configurations.
    """

    from quart import request
    from controller_layer import BESSModeWriting

    bess_mode_writer = BESSModeWriting()

    modes = await request.get_json()

    try:
        await bess_mode_writer.set_bess_modes_and_apply(modes)
        return jsonify({"status": "success", "message": "BESS control modes set and applied."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@bess_control_bp.route("/set_bess_control_setpoints", methods=["POST"])
async def set_bess_control_setpoints():
    """
    Endpoint to set BESS control setpoints.
    Expects JSON payload with mode configurations.
    """

    from quart import request
    from controller_layer import BESSModeWriting

    bess_mode_writer = BESSModeWriting()

    control_setpoints = await request.get_json()

    try:
        await bess_mode_writer.set_control_setpoints_and_apply(control_setpoints)
        return jsonify({"status": "success", "message": "BESS control control_setpoints set and applied."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bess_control_bp.route("/apply_device_control", methods=["POST"])
async def apply_device_control():
    """
    Endpoint to set Device control.
    Expects JSON payload with mode configurations.
    Expected device_controls format:
    
    {
        "plant_1_pcs_pcs_1": {
            "ppc_control": {
                "enable": true,
                "status": "on"/off
            }
        }
    }
    """

    from quart import request
    from controller_layer import BESSModeWriting

    bess_mode_writer = BESSModeWriting()

    device_controls = await request.get_json()

    try:
        await bess_mode_writer.apply_device_controls(device_controls)
        return jsonify({"status": "success", "message": "Device control set and applied."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bess_control_bp.route("/write_device_new_value", methods=["POST"])
async def write_device_new_value():
    """
    Endpoint to set Device control.
    Expects JSON payload with mode configurations.
    Expected device_controls format:
    
    {
        "block": 1,
        "device_type": "plc",
        "device_id": "plant_1_mcr_io_1",
        "alias": "spare_do_1",
        "name": "spare_do_1",
        "value": 0,
        "new_value": 1,
        "type": "analog",
        "unit": "",
        "dataType": "int16",
        "register": 300
    }
    """

    from quart import request
    from controller_layer import BESSModeWriting

    bess_mode_writer = BESSModeWriting()

    device_controls = await request.get_json()

    try:
        await bess_mode_writer.write_device_new_value(device_controls)
        return jsonify({"status": "success", "message": "Device new value set and applied."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ================= GET SETPOINT =================
@bess_control_bp.route("/get_bess_control_setpoints", methods=["GET"])
async def get_bess_control_setpoints():

    try:
        path = Path(
            DataPaths.resolve(
                "controller_layer/manual_override/bess_control_setpoints.json"
            )
        )

        if not path.exists():
            return jsonify({"status": "success", "data": {}})

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return jsonify({"status": "success", "data": data})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ================= GET MODES =================
@bess_control_bp.route("/get_bess_control_modes", methods=["GET"])
async def get_bess_control_modes():

    try:
        path = Path(
            DataPaths.resolve(
                "controller_layer/manual_override/bess_modes.json"
            )
        )

        if not path.exists():
            return jsonify({"status": "success", "data": {}})

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return jsonify({"status": "success", "data": data})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500